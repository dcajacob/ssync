from __future__ import annotations

import struct
from dataclasses import dataclass, field

from .types import Range

RANGE_STRUCT = struct.Struct("!II")


def merge_ranges(ranges: list[Range]) -> list[Range]:
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda item: item[0])
    merged: list[Range] = [ordered[0]]
    for start, end in ordered[1:]:
        current_start, current_end = merged[-1]
        if start <= current_end:
            merged[-1] = (current_start, max(current_end, end))
        else:
            merged.append((start, end))
    return merged


def ranges_from_indexes(indexes: list[int]) -> list[Range]:
    if not indexes:
        return []
    sorted_indexes = sorted(set(indexes))
    start = sorted_indexes[0]
    prev = start
    ranges: list[Range] = []
    for value in sorted_indexes[1:]:
        if value == prev + 1:
            prev = value
            continue
        ranges.append((start, prev + 1))
        start = value
        prev = value
    ranges.append((start, prev + 1))
    return ranges


def expand_ranges(ranges: list[Range]) -> list[int]:
    values: list[int] = []
    for start, end in ranges:
        values.extend(range(start, end))
    return values


def summarize_ranges(ranges: list[Range]) -> str:
    if not ranges:
        return "none"
    preview = ", ".join(f"{start}-{end}" for start, end in ranges[:3])
    if len(ranges) > 3:
        preview += ", ..."
    return f"{len(ranges)} range(s): {preview}"


def limit_ranges_to_chunk_budget(
    missing_ranges: list[Range],
    chunk_budget: int,
) -> list[Range]:
    if chunk_budget <= 0:
        return []
    remaining = chunk_budget
    limited: list[Range] = []
    for start, end in missing_ranges:
        if remaining <= 0:
            break
        length = end - start
        if length <= remaining:
            limited.append((start, end))
            remaining -= length
            continue
        limited.append((start, start + remaining))
        break
    return limited


def encode_ranges(ranges: list[Range]) -> bytes:
    payload = bytearray()
    for start, end in merge_ranges(ranges):
        payload.extend(RANGE_STRUCT.pack(start, end))
    return bytes(payload)


def decode_ranges(payload: bytes) -> list[Range]:
    if len(payload) % RANGE_STRUCT.size != 0:
        raise ValueError("Invalid range payload size")
    ranges: list[Range] = []
    for offset in range(0, len(payload), RANGE_STRUCT.size):
        start, end = RANGE_STRUCT.unpack_from(payload, offset)
        if start >= end:
            raise ValueError("Range start must be smaller than end")
        ranges.append((start, end))
    return merge_ranges(ranges)


@dataclass(slots=True)
class ChunkTracker:
    total_chunks: int
    _received_ranges: list[Range] = field(default_factory=list)
    _received_count: int = 0

    def add(self, chunk_index: int) -> bool:
        if not (0 <= chunk_index < self.total_chunks):
            return False
        if self.total_chunks == 0:
            return False

        # Locate the insertion point by range start.
        insert_at = self._find_insert_at(chunk_index)

        # Already covered by previous range.
        if insert_at > 0:
            prev_start, prev_end = self._received_ranges[insert_at - 1]
            if prev_start <= chunk_index < prev_end:
                return False

        # Already covered by next range.
        if insert_at < len(self._received_ranges):
            next_start, next_end = self._received_ranges[insert_at]
            if next_start <= chunk_index < next_end:
                return False

        new_start = chunk_index
        new_end = chunk_index + 1

        # Merge/extend previous adjacent range.
        if insert_at > 0:
            prev_start, prev_end = self._received_ranges[insert_at - 1]
            if prev_end == chunk_index:
                new_start = prev_start
                insert_at -= 1
                self._received_ranges.pop(insert_at)

        # Merge/extend next adjacent range.
        if insert_at < len(self._received_ranges):
            next_start, next_end = self._received_ranges[insert_at]
            if next_start == chunk_index + 1:
                new_end = next_end
                self._received_ranges.pop(insert_at)

        self._received_ranges.insert(insert_at, (new_start, new_end))
        self._received_count += 1
        return True

    def _find_insert_at(self, chunk_index: int) -> int:
        lo = 0
        hi = len(self._received_ranges)
        while lo < hi:
            mid = (lo + hi) // 2
            mid_start, _ = self._received_ranges[mid]
            if mid_start < chunk_index:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def is_complete(self) -> bool:
        return self._received_count == self.total_chunks

    def received_count(self) -> int:
        return self._received_count

    def missing_indexes(self) -> list[int]:
        return expand_ranges(self.missing_ranges())

    def missing_ranges(self) -> list[Range]:
        return self._missing_ranges_between(0, self.total_chunks)

    def missing_ranges_upto(self, end_exclusive: int) -> list[Range]:
        bounded_end = max(0, min(end_exclusive, self.total_chunks))
        return self._missing_ranges_between(0, bounded_end)

    def received_ranges(self) -> list[Range]:
        return list(self._received_ranges)

    def _missing_ranges_between(self, start: int, end_exclusive: int) -> list[Range]:
        if end_exclusive <= start:
            return []
        cursor = start
        missing: list[Range] = []
        for recv_start, recv_end in self._received_ranges:
            if recv_end <= cursor:
                continue
            if recv_start >= end_exclusive:
                break
            if recv_start > cursor:
                missing.append((cursor, min(recv_start, end_exclusive)))
            cursor = max(cursor, min(recv_end, end_exclusive))
            if cursor >= end_exclusive:
                break
        if cursor < end_exclusive:
            missing.append((cursor, end_exclusive))
        return missing

    @classmethod
    def from_received_ranges(cls, total_chunks: int, received_ranges: list[Range]) -> ChunkTracker:
        normalized: list[Range] = []
        for start, end in merge_ranges(received_ranges):
            bounded_start = max(0, min(start, total_chunks))
            bounded_end = max(0, min(end, total_chunks))
            if bounded_start < bounded_end:
                normalized.append((bounded_start, bounded_end))
        tracker = cls(total_chunks=total_chunks, _received_ranges=normalized)
        tracker._received_count = sum(end - start for start, end in normalized)
        return tracker

