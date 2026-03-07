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

