// SPDX-License-Identifier: AGPL-3.0-only
//! Identify legacy bank-blind streams from CreatedBank, never from a zero counter.
use yellowstone_grpc_proto::prelude::{SlotStatus, SubscribeUpdate, subscribe_update::UpdateOneof};

#[derive(Default)]
pub(super) struct Protocol {
    legacy: Option<bool>,
    pending: Vec<SubscribeUpdate>,
}

impl Protocol {
    pub(super) fn adapt(
        &mut self,
        event: Result<SubscribeUpdate, yellowstone_grpc_proto::tonic::Status>,
    ) -> Vec<Result<SubscribeUpdate, yellowstone_grpc_proto::tonic::Status>> {
        let update = match event {
            Ok(update) => update,
            Err(error) => return vec![Err(error)],
        };
        let created = match update.update_oneof.as_ref() {
            Some(UpdateOneof::Slot(slot)) if slot.status == SlotStatus::SlotCreatedBank as i32 => {
                Some(slot.bank_id.is_none())
            }
            _ => None,
        };
        if let Some(legacy) = created {
            if self.legacy.is_some_and(|old| old != legacy) {
                return vec![Err(
                    yellowstone_grpc_proto::tonic::Status::failed_precondition(
                        "bank identity protocol changed within a subscription",
                    ),
                )];
            }
            self.legacy = Some(legacy);
        }
        // Some sysvars precede CreatedBank. Hold them until its optional ID distinguishes
        // the legacy protocol from a bank-aware bank whose perfectly valid counter is zero.
        self.pending.push(update);
        let Some(legacy) = self.legacy else {
            if self.pending.len() > 1024 {
                self.pending.clear();
            }
            return Vec::new();
        };
        self.pending
            .drain(..)
            .map(|mut update| {
                if legacy {
                    let has_bank_id = match update.update_oneof.as_ref() {
                        Some(UpdateOneof::Slot(slot)) => slot.bank_id.is_some(),
                        Some(UpdateOneof::Account(account)) => account.bank_id.is_some(),
                        Some(UpdateOneof::Transaction(tx)) => tx.bank_id != 0,
                        Some(UpdateOneof::TransactionStatus(tx)) => tx.bank_id != 0,
                        Some(UpdateOneof::BlockMeta(meta)) => meta.bank_id != 0,
                        Some(UpdateOneof::Entry(entry)) => entry.bank_id != 0,
                        Some(UpdateOneof::EntryUpdateParent(_) | UpdateOneof::BlockFooter(_)) => {
                            true
                        }
                        _ => false,
                    };
                    if has_bank_id {
                        return Err(yellowstone_grpc_proto::tonic::Status::failed_precondition(
                            "bank-aware data on a legacy subscription",
                        ));
                    }

                    match update.update_oneof.as_mut() {
                        Some(UpdateOneof::Slot(slot)) => {
                            if matches!(
                                SlotStatus::try_from(slot.status),
                                Ok(SlotStatus::SlotCreatedBank
                                    | SlotStatus::SlotProcessed
                                    | SlotStatus::SlotConfirmed
                                    | SlotStatus::SlotFinalized)
                            ) {
                                slot.bank_id = Some(slot.slot);
                            }
                        }
                        Some(UpdateOneof::Account(account)) if !account.is_startup => {
                            account.bank_id = Some(account.slot)
                        }
                        Some(UpdateOneof::Transaction(tx)) => tx.bank_id = tx.slot,
                        Some(UpdateOneof::TransactionStatus(tx)) => tx.bank_id = tx.slot,
                        Some(UpdateOneof::BlockMeta(meta)) => meta.bank_id = meta.slot,
                        Some(UpdateOneof::Entry(entry)) => entry.bank_id = entry.slot,
                        _ => {}
                    }
                }
                Ok(update)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::prelude::{SubscribeUpdateSlot, SubscribeUpdateTransaction};

    fn created(bank_id: Option<u64>) -> SubscribeUpdate {
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 42,
                bank_id,
                status: SlotStatus::SlotCreatedBank as i32,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn legacy_counter_is_slot_scoped_but_bank_aware_zero_is_preserved() {
        for (bank_id, expected) in [(None, 42), (Some(0), 0)] {
            let mut protocol = Protocol::default();
            assert!(
                protocol
                    .adapt(Ok(SubscribeUpdate {
                        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                            slot: 42,
                            ..Default::default()
                        })),
                        ..Default::default()
                    }))
                    .is_empty()
            );
            let events = protocol.adapt(Ok(created(bank_id)));
            let Some(UpdateOneof::Transaction(tx)) =
                events[0].as_ref().unwrap().update_oneof.as_ref()
            else {
                panic!("transaction");
            };
            assert_eq!(tx.bank_id, expected);
        }
    }

    #[test]
    fn protocol_changes_require_a_new_connection() {
        let mut protocol = Protocol::default();
        protocol.adapt(Ok(created(None)));
        assert!(protocol.adapt(Ok(created(Some(7))))[0].is_err());
        let mut reconnect = Protocol::default();
        assert!(reconnect.adapt(Ok(created(Some(7))))[0].is_ok());
    }
}
