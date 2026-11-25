use superbank_solparq::{archive::delete_range_keeping_parent, clickhouse::SlotRange};

#[test]
fn keeps_the_last_block_at_or_before_the_range_end() {
    let range = SlotRange::new(1_000, 1_999);
    assert_eq!(
        delete_range_keeping_parent(range, Some(1_999), None, &[]),
        Some(SlotRange::new(1_000, 1_998))
    );
    // Trailing skipped slots: the parent of the next window is the last block, not the end slot.
    assert_eq!(
        delete_range_keeping_parent(range, Some(1_990), None, &[]),
        Some(SlotRange::new(1_000, 1_989))
    );
    // No block at all: nothing to keep.
    assert_eq!(
        delete_range_keeping_parent(range, None, None, &[]),
        Some(range)
    );
}

#[test]
fn nothing_is_deleted_when_the_kept_block_is_the_range_start() {
    let range = SlotRange::new(1_000, 1_999);
    assert_eq!(
        delete_range_keeping_parent(range, Some(1_000), None, &[]),
        None
    );
    assert_eq!(
        delete_range_keeping_parent(SlotRange::new(0, 10), Some(0), None, &[]),
        None
    );
}

#[test]
fn widens_down_to_the_previously_kept_block_only_when_an_archive_covers_it() {
    let range = SlotRange::new(2_000, 2_999);
    let archived = [(1_000, 1_999), (2_000, 2_999)];
    assert_eq!(
        delete_range_keeping_parent(range, Some(2_999), Some(1_999), &archived),
        Some(SlotRange::new(1_999, 2_998))
    );
    // The first window of an aligned kind: its parent is in the never-archived prefix.
    assert_eq!(
        delete_range_keeping_parent(range, Some(2_999), Some(1_999), &[(2_000, 2_999)]),
        Some(SlotRange::new(2_000, 2_998))
    );
    // A parent inside the range is already covered by it.
    assert_eq!(
        delete_range_keeping_parent(range, Some(2_999), Some(2_500), &archived),
        Some(SlotRange::new(2_000, 2_998))
    );
}
