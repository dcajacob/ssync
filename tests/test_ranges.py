from ssync.space_sync.ranges import ChunkTracker, decode_ranges, encode_ranges, merge_ranges


def test_merge_ranges_combines_overlaps_and_adjacent() -> None:
    merged = merge_ranges([(0, 2), (2, 4), (6, 8), (7, 9)])
    assert merged == [(0, 4), (6, 9)]


def test_range_encoding_round_trip() -> None:
    original = [(0, 3), (7, 10)]
    decoded = decode_ranges(encode_ranges(original))
    assert decoded == original


def test_chunk_tracker_reports_missing_ranges() -> None:
    tracker = ChunkTracker(total_chunks=8)
    for value in (0, 1, 3, 4, 7):
        tracker.add(value)
    assert tracker.missing_ranges() == [(2, 3), (5, 7)]


def test_chunk_tracker_merges_adjacent_ranges_incrementally() -> None:
    tracker = ChunkTracker(total_chunks=10)
    assert tracker.add(3) is True
    assert tracker.add(5) is True
    assert tracker.add(4) is True
    assert tracker.received_ranges() == [(3, 6)]
    assert tracker.received_count() == 3
    assert tracker.add(4) is False


def test_chunk_tracker_missing_ranges_upto_uses_received_ranges() -> None:
    tracker = ChunkTracker(total_chunks=12)
    for value in (0, 1, 2, 6, 7, 10):
        assert tracker.add(value) is True
    assert tracker.missing_ranges_upto(8) == [(3, 6)]
    assert tracker.missing_ranges() == [(3, 6), (8, 10), (11, 12)]


def test_chunk_tracker_restore_from_received_ranges() -> None:
    tracker = ChunkTracker.from_received_ranges(
        total_chunks=8,
        received_ranges=[(-2, 2), (2, 4), (6, 9)],
    )
    assert tracker.received_ranges() == [(0, 4), (6, 8)]
    assert tracker.received_count() == 6
    assert tracker.missing_ranges() == [(4, 6)]

