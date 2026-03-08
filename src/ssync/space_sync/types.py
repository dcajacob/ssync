from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path

PROTOCOL_MAGIC = b"SS"
PROTOCOL_VERSION = 1
TRANSFER_ID_SIZE = 16
SHA256_SIZE = 32
MAX_TLV_VALUE_SIZE = 65535
MAX_TLV_TOTAL_SIZE = 65535
MAX_FILE_NAME_BYTES = 4096
MAX_RANGES_PER_FRAME = 2048
DEFAULT_CHUNK_SIZE = 1024
DEFAULT_MANIFEST_REPEATS = 3
DEFAULT_SOCKET_TIMEOUT = 0.5


class FrameType(IntEnum):
    MANIFEST = 1
    DATA = 2
    FIN = 3
    STATUS = 4
    REPAIR_REQUEST = 5
    REPAIR_DONE = 6
    FILE_INFO_REQUEST = 7
    FILE_INFO_RESPONSE = 8
    TRANSFER_COMPLETE = 9


class TransferState(IntEnum):
    INCOMPLETE = 0
    COMPLETE = 1
    HASH_MISMATCH = 2


class MetadataType(IntEnum):
    MISSION_TAG = 1
    USER_NOTE = 2
    SOURCE_MTIME_NS = 3


Range = tuple[int, int]


@dataclass(slots=True)
class SenderConfig:
    chunk_size: int = DEFAULT_CHUNK_SIZE
    manifest_repeats: int = DEFAULT_MANIFEST_REPEATS
    inter_packet_delay_s: float = 0.0002
    enable_feedback: bool = False
    feedback_wait_s: float = 2.0
    max_repair_rounds: int = 32
    max_feedback_idle_timeouts: int = 2
    drop_every_nth_data: int = 0
    max_data_rate_bps: int = 0
    midstream_repair_max_rounds_per_poll: int = 1
    midstream_repair_max_chunks_per_poll: int = 512
    repair_duplicate_suppression_s: float = 0.2


@dataclass(slots=True)
class ReceiverConfig:
    output_dir: Path
    enable_feedback: bool = False
    keep_part_files_on_complete: bool = False
    status_repeat: int = 3
    periodic_repair_request_s: float = 0.5
    periodic_repair_min_seen_chunks: int = 32
    max_repair_chunks_per_request: int = 256
    transfer_inactivity_timeout_s: float = 10.0
    socket_rcvbuf_bytes: int = 8 * 1024 * 1024
    journal_flush_interval_s: float = 0.5
    repair_request_cooldown_s: float = 0.2
    repair_request_inflight_timeout_s: float = 1.5


@dataclass(slots=True)
class SendResult:
    transfer_id_hex: str
    total_chunks: int
    repaired_chunks: int = 0
    repair_rounds: int = 0
    completed: bool = True


@dataclass(slots=True)
class ReceivedTransferInfo:
    transfer_id_hex: str
    file_name: str
    completed: bool
    missing_ranges: list[Range] = field(default_factory=list)
    hash_mismatch: bool = False


@dataclass(slots=True)
class RemoteFileInfo:
    path: str
    exists: bool
    size: int = 0
    mtime_ns: int = 0
    sha256: bytes | None = None

