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
DEFAULT_CHUNK_SIZE = 4096
DEFAULT_MANIFEST_REPEATS = 3
DEFAULT_METADATA_REPEATS = DEFAULT_MANIFEST_REPEATS
DEFAULT_SOCKET_TIMEOUT = 0.5


class FrameType(IntEnum):
    METADATA = 1
    DATA = 2
    STATUS = 4
    BEACON = 10


class BeaconRole(IntEnum):
    SENDER = 1
    RECEIVER = 2


class TransferState(IntEnum):
    INCOMPLETE = 0
    COMPLETE = 1
    HASH_MISMATCH = 2
    PENDING_METADATA = 3
    KEEPALIVE = 4


class StatusKind(IntEnum):
    TRANSFER = 0
    FILE_INFO_RESPONSE = 1


class MetadataType(IntEnum):
    MISSION_TAG = 1
    USER_NOTE = 2
    SOURCE_MTIME_NS = 3
    REPLY_PORT = 4
    FILE_INFO_QUERY_PATH = 100
    FILE_INFO_QUERY_INCLUDE_CHECKSUM = 101
    FILE_INFO_QUERY_TOKEN = 102


Range = tuple[int, int]


@dataclass(slots=True)
class SenderConfig:
    chunk_size: int = DEFAULT_CHUNK_SIZE
    manifest_repeats: int = DEFAULT_MANIFEST_REPEATS
    inter_packet_delay_s: float = 0.0
    enable_feedback: bool = False
    auto_feedback_discovery: bool = True
    auto_feedback_idle_timeout_s: float = 60.0
    auto_feedback_probe_interval_chunks: int = 64
    feedback_wait_s: float = 5.0
    max_repair_rounds: int = 32
    max_feedback_idle_timeouts: int = 2
    drop_every_nth_data: int = 0
    drop_rate: float = 0.0
    max_data_rate_bps: int = 0
    midstream_repair_max_rounds_per_poll: int = 1
    midstream_repair_max_chunks_per_poll: int = 512
    repair_duplicate_suppression_s: float = 0.2
    beacon_interval_s: float = 1.0
    periodic_metadata_interval_s: float = 10.0
    periodic_metadata_every_n_chunks: int = 0
    revisit_incomplete_passes: int = 2
    revisit_max_rounds_per_pass: int = 8
    primary_feedback_max_rounds: int = 0
    primary_feedback_max_seconds: float = 0.0
    repair_queue_max_pending_requests: int = 1024
    repair_worker_max_chunks_per_burst: int = 256
    initial_pass_repair_max_chunks_per_burst: int = 16
    repair_worker_poll_interval_s: float = 0.01

    @property
    def metadata_repeats(self) -> int:
        return self.manifest_repeats


@dataclass(slots=True)
class ReceiverConfig:
    output_dir: Path
    enable_feedback: bool = False
    keep_part_files_on_complete: bool = False
    status_repeat: int = 3
    periodic_repair_request_s: float = 0.5
    periodic_repair_min_seen_chunks: int = 32
    max_repair_chunks_per_request: int = 256
    adaptive_leading_hole_boost: bool = True
    leading_hole_start_threshold_chunks: int = 512
    leading_hole_min_span_chunks: int = 2048
    leading_hole_boost_multiplier: int = 4
    leading_hole_max_repair_chunks_per_request: int = 2048
    transfer_inactivity_timeout_s: float = 10.0
    socket_rcvbuf_bytes: int = 8 * 1024 * 1024
    journal_flush_interval_s: float = 0.5
    repair_request_cooldown_s: float = 0.2
    repair_request_inflight_timeout_s: float = 1.5
    beacon_interval_s: float = 1.0
    pre_metadata_max_pending_bytes: int = 8 * 1024 * 1024
    pre_metadata_max_pending_bytes_per_transfer: int = 512 * 1024
    pre_metadata_max_pending_transfers: int = 128
    pre_metadata_ttl_s: float = 30.0
    forward_stream_quiet_s: float = 0.5
    monitor_ipc_socket: Path | None = None


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

