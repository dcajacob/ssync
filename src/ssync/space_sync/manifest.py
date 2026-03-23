from __future__ import annotations

import hashlib
import math
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .types import Range


@dataclass(slots=True)
class TransferManifest:
    transfer_id: bytes
    file_name: str
    file_size: int
    chunk_size: int
    total_chunks: int
    sha256: bytes
    metadata: dict[int, bytes] = field(default_factory=dict)

    @classmethod
    def from_file(
        cls,
        file_path: Path,
        chunk_size: int,
        remote_name: str | None = None,
        metadata: dict[int, bytes] | None = None,
    ) -> TransferManifest:
        raw = file_path.read_bytes()
        return cls.from_bytes(
            raw=raw,
            file_name=remote_name or file_path.name,
            chunk_size=chunk_size,
            metadata=metadata,
        )

    @classmethod
    def from_bytes(
        cls,
        *,
        raw: bytes,
        file_name: str,
        chunk_size: int,
        metadata: dict[int, bytes] | None = None,
    ) -> TransferManifest:
        file_size = len(raw)
        total_chunks = math.ceil(file_size / chunk_size) if file_size else 0
        return cls(
            transfer_id=uuid.uuid4().bytes,
            file_name=file_name,
            file_size=file_size,
            chunk_size=chunk_size,
            total_chunks=total_chunks,
            sha256=hashlib.sha256(raw).digest(),
            metadata=metadata or {},
        )


@dataclass(slots=True)
class RepairRequest:
    transfer_id: bytes
    missing_ranges: list[Range]


# Compatibility alias: METADATA terminology without changing wire format.
TransferMetadata = TransferManifest

