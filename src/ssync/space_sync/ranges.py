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
    _received: set[int] = field(default_factory=set)

    def add(self, chunk_index: int) -> None:
        if 0 <= chunk_index < self.total_chunks:
            self._received.add(chunk_index)

    def is_complete(self) -> bool:
        return len(self._received) == self.total_chunks

    def missing_indexes(self) -> list[int]:
        if self.total_chunks == 0:
            return []
        return [index for index in range(self.total_chunks) if index not in self._received]

    def missing_ranges(self) -> list[Range]:
        return ranges_from_indexes(self.missing_indexes())

