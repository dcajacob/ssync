from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from ssync.space_sync.frames import (
    HEADER_STRUCT,
    TransferStatus,
    decode_frame,
    decode_status,
    encode_status,
)
from ssync.space_sync.ranges import (
    ChunkTracker,
    clamp_ranges_to_chunks,
    decode_ranges,
    encode_ranges,
    merge_ranges,
)
from ssync.space_sync.types import FrameType, StatusKind, TransferState

_range_strategy = st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=10_000),
        st.integers(min_value=0, max_value=10_000),
    ).filter(lambda item: item[0] < item[1]),
    max_size=64,
)


@settings(max_examples=100)
@given(_range_strategy)
def test_range_encoding_round_trip_property(ranges: list[tuple[int, int]]) -> None:
    decoded = decode_ranges(encode_ranges(ranges))

    assert decoded == merge_ranges(ranges)


@settings(max_examples=100)
@given(_range_strategy, st.integers(min_value=0, max_value=10_000))
def test_clamped_ranges_stay_within_total_chunks(
    ranges: list[tuple[int, int]],
    total_chunks: int,
) -> None:
    clamped = clamp_ranges_to_chunks(ranges, total_chunks)

    assert clamped == merge_ranges(clamped)
    assert all(0 <= start < end <= total_chunks for start, end in clamped)


@settings(max_examples=100)
@given(
    st.integers(min_value=0, max_value=512),
    st.lists(st.integers(min_value=-100, max_value=700), max_size=1024),
)
def test_chunk_tracker_counts_unique_in_bounds_indexes(
    total_chunks: int,
    indexes: list[int],
) -> None:
    tracker = ChunkTracker(total_chunks=total_chunks)

    for index in indexes:
        tracker.add(index)

    expected = {index for index in indexes if 0 <= index < total_chunks}
    assert tracker.received_count() == len(expected)
    assert tracker.is_complete() is (len(expected) == total_chunks)
    for start, end in tracker.received_ranges():
        assert 0 <= start < end <= total_chunks


@settings(max_examples=100)
@given(_range_strategy)
def test_status_frame_round_trip_property(ranges: list[tuple[int, int]]) -> None:
    status = TransferStatus(
        transfer_id=b"\x01" * 16,
        kind=StatusKind.TRANSFER,
        state=TransferState.INCOMPLETE,
        missing_ranges=ranges,
    )

    decoded = decode_status(decode_frame(encode_status(status)).payload)

    assert decoded.transfer_id == status.transfer_id
    assert decoded.kind == status.kind
    assert decoded.state == status.state
    assert decoded.missing_ranges == merge_ranges(ranges)


@settings(max_examples=100)
@given(st.integers(min_value=0, max_value=255).filter(lambda value: value not in {1, 2, 4, 10}))
def test_unknown_frame_types_decode_without_exception(frame_type: int) -> None:
    payload = b"payload"
    raw = HEADER_STRUCT.pack(b"SS", 1, frame_type, 0, 0, len(payload)) + payload

    parsed = decode_frame(raw)

    assert parsed.frame_type is None
    assert parsed.frame_type_raw == frame_type


def test_frame_type_strategy_covers_known_values() -> None:
    assert {item.value for item in FrameType} == {1, 2, 4, 10}
