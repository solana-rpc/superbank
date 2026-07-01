// SPDX-License-Identifier: AGPL-3.0-only
/*
 * Copyright 2025-2026 Triton One Limited. All rights reserved.
 */

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use futures_util::Stream;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use crate::clickhouse::{
    BlockMetadataRecord, StoredTransactionRecord, transient_shard_local_error_reason,
};
use crate::grpc::generated::superbank::{
    BlockRequest, BlockResponse, BlockTimeRequest, BlockTimeResponse, GetRequest, GetResponse,
    StreamBlocksRequest, StreamTransactionsRequest, TransactionRequest, TransactionResponse,
    VersionRequest, VersionResponse,
    superbank_server::{Superbank, SuperbankServer},
};
use crate::grpc::wire::{
    AccountFilters, block_matches_account_filter, encode_block_response,
    encode_transaction_response, transaction_matches_stream_filter,
};
use crate::metrics;
use crate::processing::ProcessingError;
use crate::state::AppState;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

const STREAM_BLOCKS_METHOD: &str = "StreamBlocks";
const STREAM_TRANSACTIONS_METHOD: &str = "StreamTransactions";

#[derive(Clone, Debug)]
pub(crate) struct SuperbankGrpcConfig {
    pub(crate) max_slot_range: u64,
    pub(crate) query_timeout: Duration,
    pub(crate) chunk_slots: u64,
    pub(crate) max_send_bytes: usize,
    pub(crate) max_concurrent_streams: u32,
}

#[derive(Clone)]
pub(crate) struct SuperbankGrpcService {
    state: Arc<AppState>,
    config: SuperbankGrpcConfig,
}

impl SuperbankGrpcService {
    pub(crate) fn new(state: Arc<AppState>, config: SuperbankGrpcConfig) -> Self {
        Self { state, config }
    }
}

pub(crate) async fn serve(
    state: Arc<AppState>,
    config: SuperbankGrpcConfig,
    listener: TcpListener,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), tonic::transport::Error> {
    let service = SuperbankGrpcService::new(state, config.clone());
    let service = SuperbankServer::new(service).max_encoding_message_size(config.max_send_bytes);

    Server::builder()
        .max_concurrent_streams(Some(config.max_concurrent_streams))
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let _ = shutdown_rx.recv().await;
        })
        .await
}

#[tonic::async_trait]
impl Superbank for SuperbankGrpcService {
    async fn get_version(
        &self,
        _request: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        Err(unimplemented_status())
    }

    async fn get_block(
        &self,
        _request: Request<BlockRequest>,
    ) -> Result<Response<BlockResponse>, Status> {
        Err(unimplemented_status())
    }

    async fn get_block_time(
        &self,
        _request: Request<BlockTimeRequest>,
    ) -> Result<Response<BlockTimeResponse>, Status> {
        Err(unimplemented_status())
    }

    async fn get_transaction(
        &self,
        _request: Request<TransactionRequest>,
    ) -> Result<Response<TransactionResponse>, Status> {
        Err(unimplemented_status())
    }

    type GetStream = BoxStream<GetResponse>;

    async fn get(
        &self,
        _request: Request<tonic::Streaming<GetRequest>>,
    ) -> Result<Response<Self::GetStream>, Status> {
        Err(unimplemented_status())
    }

    type StreamBlocksStream = BoxStream<BlockResponse>;

    async fn stream_blocks(
        &self,
        request: Request<StreamBlocksRequest>,
    ) -> Result<Response<Self::StreamBlocksStream>, Status> {
        let request = request.into_inner();
        let (start_slot, end_slot) = resolve_slot_range(
            request.start_slot,
            request.end_slot,
            self.config.max_slot_range,
        )?;
        let account_filter = AccountFilters::for_blocks(
            request
                .filter
                .as_ref()
                .map(|filter| filter.account_include.as_slice())
                .unwrap_or_default(),
        )?;
        let state = self.state.clone();
        let config = self.config.clone();
        metrics::superbank_grpc_stream_started(STREAM_BLOCKS_METHOD);

        let stream = try_stream! {
            let mut chunk_start = start_slot;
            while chunk_start <= end_slot {
                let chunk_end = bounded_chunk_end(chunk_start, end_slot, config.chunk_slots);
                let (metadata, transactions) = fetch_chunk(&state, chunk_start, chunk_end, config.query_timeout)
                    .await
                    .inspect_err(|_| {
                        metrics::superbank_grpc_stream_error(STREAM_BLOCKS_METHOD, "clickhouse");
                    })?;
                metrics::superbank_grpc_stream_chunk(STREAM_BLOCKS_METHOD);
                let mut tx_by_slot = transactions_by_slot(transactions);

                for metadata in metadata {
                    let txs = tx_by_slot.remove(&metadata.slot).unwrap_or_default();
                    if block_matches_account_filter(&txs, &account_filter) {
                        let response = encode_block_response_blocking(&state, metadata, txs)
                            .await
                            .inspect_err(|_| {
                                metrics::superbank_grpc_stream_error(STREAM_BLOCKS_METHOD, "encode");
                            })?;
                        metrics::superbank_grpc_stream_message(STREAM_BLOCKS_METHOD);
                        yield response;
                    }
                }

                if chunk_end == u64::MAX {
                    break;
                }
                chunk_start = chunk_end + 1;
            }
        };

        Ok(Response::new(Box::pin(stream) as Self::StreamBlocksStream))
    }

    type StreamTransactionsStream = BoxStream<TransactionResponse>;

    async fn stream_transactions(
        &self,
        request: Request<StreamTransactionsRequest>,
    ) -> Result<Response<Self::StreamTransactionsStream>, Status> {
        let request = request.into_inner();
        let (start_slot, end_slot) = resolve_slot_range(
            request.start_slot,
            request.end_slot,
            self.config.max_slot_range,
        )?;
        let account_filters = AccountFilters::for_transactions(request.filter.as_ref())?;
        let state = self.state.clone();
        let config = self.config.clone();
        let filter = request.filter;
        metrics::superbank_grpc_stream_started(STREAM_TRANSACTIONS_METHOD);

        let stream = try_stream! {
            let mut chunk_start = start_slot;
            while chunk_start <= end_slot {
                let chunk_end = bounded_chunk_end(chunk_start, end_slot, config.chunk_slots);
                let (transactions, _) = state
                    .clickhouse
                    .get_block_full_transactions_by_slot_range(chunk_start, chunk_end, config.query_timeout)
                    .await
                    .map_err(status_from_processing_error)
                    .inspect_err(|_| {
                        metrics::superbank_grpc_stream_error(STREAM_TRANSACTIONS_METHOD, "clickhouse");
                    })?;
                metrics::superbank_grpc_stream_chunk(STREAM_TRANSACTIONS_METHOD);

                for record in transactions {
                    if transaction_matches_stream_filter(&record, filter.as_ref(), &account_filters) {
                        let response = encode_transaction_response_blocking(&state, record)
                            .await
                            .inspect_err(|_| {
                                metrics::superbank_grpc_stream_error(
                                    STREAM_TRANSACTIONS_METHOD,
                                    "encode",
                                );
                            })?;
                        metrics::superbank_grpc_stream_message(STREAM_TRANSACTIONS_METHOD);
                        yield response;
                    }
                }

                if chunk_end == u64::MAX {
                    break;
                }
                chunk_start = chunk_end + 1;
            }
        };

        Ok(Response::new(
            Box::pin(stream) as Self::StreamTransactionsStream
        ))
    }
}

fn resolve_slot_range(
    start_slot: u64,
    end_slot: Option<u64>,
    max_slot_range: u64,
) -> Result<(u64, u64), Status> {
    let max_slot_range = max_slot_range.max(1);
    let end_slot = match end_slot {
        Some(end_slot) => end_slot,
        None => start_slot.saturating_add(max_slot_range.saturating_sub(1)),
    };

    if end_slot < start_slot {
        return Err(Status::new(
            Code::InvalidArgument,
            "end_slot must be greater than or equal to start_slot",
        ));
    }

    let width = end_slot
        .checked_sub(start_slot)
        .and_then(|delta| delta.checked_add(1))
        .ok_or_else(|| Status::new(Code::InvalidArgument, "slot range is too wide"))?;
    if width > max_slot_range {
        return Err(Status::new(
            Code::InvalidArgument,
            format!("slot range exceeds maximum of {max_slot_range} slots"),
        ));
    }

    Ok((start_slot, end_slot))
}

fn bounded_chunk_end(start_slot: u64, end_slot: u64, chunk_slots: u64) -> u64 {
    let chunk_slots = chunk_slots.max(1);
    start_slot
        .saturating_add(chunk_slots.saturating_sub(1))
        .min(end_slot)
}

async fn fetch_chunk(
    state: &Arc<AppState>,
    start_slot: u64,
    end_slot: u64,
    query_timeout: Duration,
) -> Result<(Vec<BlockMetadataRecord>, Vec<StoredTransactionRecord>), Status> {
    let (metadata, transactions) = tokio::try_join!(
        state
            .clickhouse
            .get_block_metadata_by_slot_range(start_slot, end_slot, query_timeout),
        state.clickhouse.get_block_full_transactions_by_slot_range(
            start_slot,
            end_slot,
            query_timeout
        ),
    )
    .map_err(status_from_processing_error)?;

    Ok((metadata.0, transactions.0))
}

fn transactions_by_slot(
    transactions: Vec<StoredTransactionRecord>,
) -> BTreeMap<u64, Vec<StoredTransactionRecord>> {
    let mut by_slot = BTreeMap::<u64, Vec<StoredTransactionRecord>>::new();
    for transaction in transactions {
        by_slot
            .entry(transaction.slot)
            .or_default()
            .push(transaction);
    }
    by_slot
}

async fn encode_block_response_blocking(
    state: &Arc<AppState>,
    metadata: BlockMetadataRecord,
    transactions: Vec<StoredTransactionRecord>,
) -> Result<BlockResponse, Status> {
    let permit = state
        .hydration_sem
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| Status::new(Code::Internal, "hydration semaphore closed"))?;

    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        encode_block_response(metadata, transactions)
    })
    .await
    .map_err(|err| {
        Status::new(
            Code::Internal,
            format!("failed to join gRPC block encoding task: {err}"),
        )
    })?
}

async fn encode_transaction_response_blocking(
    state: &Arc<AppState>,
    record: StoredTransactionRecord,
) -> Result<TransactionResponse, Status> {
    let permit = state
        .hydration_sem
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| Status::new(Code::Internal, "hydration semaphore closed"))?;

    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        encode_transaction_response(record)
    })
    .await
    .map_err(|err| {
        Status::new(
            Code::Internal,
            format!("failed to join gRPC transaction encoding task: {err}"),
        )
    })?
}

fn status_from_processing_error(err: ProcessingError) -> Status {
    let code = match &err {
        ProcessingError::Timeout { .. } => Code::DeadlineExceeded,
        ProcessingError::Database { .. } if transient_shard_local_error_reason(&err).is_some() => {
            Code::Unavailable
        }
        ProcessingError::Database { .. } | ProcessingError::Deserialization { .. } => {
            Code::Internal
        }
    };

    Status::new(code, err.to_string())
}

fn unimplemented_status() -> Status {
    Status::new(
        Code::Unimplemented,
        "method is not implemented in Superbank gRPC v1",
    )
}

#[cfg(test)]
mod tests {
    use super::{bounded_chunk_end, resolve_slot_range, status_from_processing_error};
    use crate::processing::ProcessingError;
    use tonic::Code;

    #[test]
    fn resolve_slot_range_defaults_to_bounded_range() {
        assert_eq!(resolve_slot_range(10, None, 5).unwrap(), (10, 14));
    }

    #[test]
    fn resolve_slot_range_rejects_inverted_range() {
        let err = resolve_slot_range(10, Some(9), 5).expect_err("invalid range");
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[test]
    fn resolve_slot_range_rejects_oversized_range() {
        let err = resolve_slot_range(10, Some(15), 5).expect_err("invalid range");
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[test]
    fn bounded_chunk_end_caps_to_requested_end() {
        assert_eq!(bounded_chunk_end(10, 12, 8), 12);
    }

    #[test]
    fn status_from_processing_error_maps_timeouts_to_deadline_exceeded() {
        let status = status_from_processing_error(ProcessingError::timeout_msg("chunk timed out"));
        assert_eq!(status.code(), Code::DeadlineExceeded);
    }

    #[test]
    fn status_from_processing_error_maps_transient_database_errors_to_unavailable() {
        let status =
            status_from_processing_error(ProcessingError::database_msg("Shard 1 tcp handle error"));
        assert_eq!(status.code(), Code::Unavailable);
    }
}
