from __future__ import annotations

import errno
import hashlib
import json
import logging
import mmap
import os
import queue as queue_mod
import shutil
import socket
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .frames import (
    BEACON_PEER_AGE_NEVER,
    TransferStatus,
    decode_beacon,
    decode_data_chunk,
    decode_frame,
    decode_manifest,
    encode_beacon,
    encode_status,
)
from .manifest import TransferManifest
from .output_dir import safe_output_path
from .ranges import ChunkTracker, limit_ranges_to_chunk_budget, summarize_ranges
from .types import (
    DEFAULT_SOCKET_TIMEOUT,
    TRANSFER_ID_SIZE,
    BeaconRole,
    FrameType,
    MetadataType,
    ReceivedTransferInfo,
    ReceiverConfig,
    RemoteFileInfo,
    StatusKind,
    TransferState,
)

LOGGER = logging.getLogger(__name__)
_COMPLETED_HASH_CACHE_MAX_ENTRIES = 4096
_REPAIR_PROGRESS_EARLY_RELEASE_RATIO = 0.75
_MONITOR_IPC_MIN_UPDATE_INTERVAL_S = 0.05
@dataclass(slots=True)
class _TransferStateData:
    manifest: TransferManifest
    part_path: Path
    final_path: Path
    tracker: ChunkTracker
    source_addr: tuple[str, int]
    reply_addr: tuple[str, int] | None = None
    done: bool = False
    finalized: bool = False
    hash_mismatch: bool = False
    highest_chunk_seen: int = -1
    last_chunk_seen: int = -1
    last_periodic_repair_request_s: float = 0.0
    last_repair_done_s: float = 0.0
    repair_request_in_flight: bool = False
    received_count_at_last_request: int = 0
    requested_chunks_at_last_request: int = 0
    last_activity_s: float = 0.0
    last_data_s: float = 0.0
    mapped_stream: BinaryIO | None = None
    mapped_file: mmap.mmap | None = None
    mmap_dirty: bool = False
    last_mmap_flush_s: float = 0.0
    last_beacon_s: float = 0.0
    last_beacon_rx_s: float = 0.0
    last_sender_peer_age_ms: int = BEACON_PEER_AGE_NEVER
    backfill_chunks: int = 0
    last_monitor_publish_s: float = 0.0


@dataclass(slots=True)
class _PendingPreMetadata:
    chunks: dict[int, bytes]
    buffered_bytes: int = 0
    last_update_s: float = 0.0


class SpaceSyncReceiver:
    def __init__(
        self,
        bind_host: str,
        bind_port: int,
        config: ReceiverConfig,
    ) -> None:
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.config = config
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._transfers: dict[bytes, _TransferStateData] = {}
        self._transfer_ids_by_signature: dict[tuple[int, int, bytes, str], bytes] = {}
        self._transfer_ids_by_final_path: dict[Path, bytes] = {}
        self._active_allocation_bytes = 0
        self._completed: list[ReceivedTransferInfo] = []
        self._thread: threading.Thread | None = None
        self._maintenance_thread: threading.Thread | None = None
        self._finalize_thread: threading.Thread | None = None
        self._finalize_queue: queue_mod.Queue[
            tuple[socket.socket, _TransferStateData, list[tuple[int, int]]]
        ] = queue_mod.Queue()
        self._send_queue: queue_mod.Queue[
            tuple[bytes, tuple[str, int]]
        ] = queue_mod.Queue(maxsize=4096)
        self._send_thread: threading.Thread | None = None
        self._repair_timer_thread: threading.Thread | None = None
        self._tx_sock: socket.socket | None = None
        self._journal_dirty = False
        self._last_journal_flush_s = 0.0
        self._completed_hash_cache: dict[Path, tuple[int, int, bytes]] = {}
        self._pending_pre_metadata: dict[bytes, _PendingPreMetadata] = {}
        self._pending_pre_metadata_total_bytes = 0
        self._monitor_ipc_send_sock: socket.socket | None = None

    @property
    def completed_transfers(self) -> list[ReceivedTransferInfo]:
        with self._lock:
            return list(self._completed)

    def start(self) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        if self._thread and self._thread.is_alive():
            return
        self._load_journal()
        LOGGER.info(
            (
                "receiver start bind=%s:%d feedback=%s periodic=%.3fs "
                "max_repair_chunks=%d cooldown=%.3fs inflight_timeout=%.3fs "
                "inactivity=%.2fs rcvbuf=%d journal_flush=%.3fs "
                "leading_hole(boost=%s threshold=%d span=%d mult=%d cap=%d) "
                "pre_metadata(global=%d per_transfer=%d transfers=%d ttl=%.1fs)"
            ),
            self.bind_host,
            self.bind_port,
            self.config.enable_feedback,
            self.config.periodic_repair_request_s,
            self.config.max_repair_chunks_per_request,
            self.config.repair_request_cooldown_s,
            self.config.repair_request_inflight_timeout_s,
            self.config.transfer_inactivity_timeout_s,
            self.config.socket_rcvbuf_bytes,
            self.config.journal_flush_interval_s,
            self.config.adaptive_leading_hole_boost,
            self.config.leading_hole_start_threshold_chunks,
            self.config.leading_hole_min_span_chunks,
            self.config.leading_hole_boost_multiplier,
            self.config.leading_hole_max_repair_chunks_per_request,
            self.config.pre_metadata_max_pending_bytes,
            self.config.pre_metadata_max_pending_bytes_per_transfer,
            self.config.pre_metadata_max_pending_transfers,
            self.config.pre_metadata_ttl_s,
        )
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        self._maintenance_thread = threading.Thread(
            target=self._run_maintenance_loop,
            daemon=True,
        )
        self._maintenance_thread.start()
        self._finalize_thread = threading.Thread(
            target=self._run_finalize_worker,
            name="ssync-finalize-worker",
            daemon=True,
        )
        self._finalize_thread.start()
        self._tx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tx_sock.setblocking(False)
        self._send_thread = threading.Thread(
            target=self._run_send_worker,
            name="ssync-send-worker",
            daemon=True,
        )
        self._send_thread.start()
        if self.config.enable_feedback and self.config.periodic_repair_request_s > 0:
            self._repair_timer_thread = threading.Thread(
                target=self._run_repair_timer,
                name="ssync-repair-timer",
                daemon=True,
            )
            self._repair_timer_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        if self._maintenance_thread:
            self._maintenance_thread.join(timeout=2.0)
        if self._finalize_thread:
            self._finalize_thread.join(timeout=5.0)
        if self._repair_timer_thread:
            self._repair_timer_thread.join(timeout=2.0)
        if self._send_thread:
            self._send_thread.join(timeout=2.0)
        if self._tx_sock is not None:
            try:
                self._tx_sock.close()
            except OSError:
                pass
            self._tx_sock = None
        with self._lock:
            for transfer in self._transfers.values():
                self._close_transfer_mmap(transfer)
            self._flush_journal_locked(force=True)
        if self._monitor_ipc_send_sock is not None:
            try:
                self._monitor_ipc_send_sock.close()
            except OSError:
                pass
            self._monitor_ipc_send_sock = None
        LOGGER.info("receiver stopped bind=%s:%d", self.bind_host, self.bind_port)

    def run(self) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            if self.config.socket_rcvbuf_bytes > 0:
                sock.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_RCVBUF,
                    self.config.socket_rcvbuf_bytes,
                )
            sock.bind((self.bind_host, self.bind_port))
            sock.settimeout(DEFAULT_SOCKET_TIMEOUT)
            if self.config.socket_rcvbuf_bytes > 0:
                effective = int(sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF))
                LOGGER.info("receiver effective SO_RCVBUF=%d", effective)
                if effective < self.config.socket_rcvbuf_bytes:
                    LOGGER.warning(
                        "requested SO_RCVBUF=%d but effective=%d (kernel cap); "
                        "increase net.core.rmem_max/rmem_default",
                        self.config.socket_rcvbuf_bytes,
                        effective,
                    )
            while not self._stop_event.is_set():
                try:
                    raw, source_addr = sock.recvfrom(65535)
                except TimeoutError:
                    self._tick_periodic_repairs(sock)
                    continue
                try:
                    frame = decode_frame(raw)
                except ValueError:
                    continue
                if frame.frame_type is None:
                    continue
                self._handle_frame(sock, frame.frame_type, frame.payload, source_addr)

    def _run_maintenance_loop(self) -> None:
        while not self._stop_event.is_set():
            time.sleep(0.05)
            with self._lock:
                active_transfers = list(self._transfers.values())
                self._evict_expired_pre_metadata_locked()
                self._flush_journal_locked(force=False)
                for transfer in active_transfers:
                    self._flush_transfer_mmap(transfer, force=False)

    def _handle_frame(
        self,
        sock: socket.socket,
        frame_type: FrameType,
        payload: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        if frame_type == FrameType.METADATA:
            try:
                manifest = decode_manifest(payload)
            except ValueError:
                return
            if self._maybe_handle_file_info_query(sock, manifest, source_addr):
                return
            self._prepare_transfer(sock, manifest, source_addr)
            return
        if frame_type == FrameType.DATA:
            try:
                chunk = decode_data_chunk(payload)
            except ValueError:
                return
            self._accept_data(
                sock,
                chunk.transfer_id,
                chunk.chunk_index,
                chunk.payload,
                source_addr=source_addr,
            )
            return
        if frame_type == FrameType.BEACON:
            try:
                _role, transfer_id, peer_age = decode_beacon(payload)
            except ValueError:
                return
            with self._lock:
                transfer = self._transfers.get(transfer_id)
                if transfer is None or transfer.finalized:
                    return
                transfer.source_addr = source_addr
                transfer.last_activity_s = time.monotonic()
                transfer.last_beacon_rx_s = transfer.last_activity_s
                transfer.last_sender_peer_age_ms = peer_age
                self._mark_journal_dirty_locked()
                self._publish_beacon_event_locked(
                    transfer,
                    direction="rx",
                    timestamp_s=transfer.last_activity_s,
                )
            return

    def _prepare_transfer(
        self,
        sock: socket.socket,
        manifest: TransferManifest,
        source_addr: tuple[str, int],
    ) -> None:
        final_path = self._safe_destination_path(manifest.file_name)
        if final_path is None:
            return
        if not self._admit_manifest(manifest):
            return
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if self._is_matching_completed_file(final_path, manifest):
            if self.config.enable_feedback:
                self._sendto_best_effort(
                    sock,
                    encode_status(
                        TransferStatus(
                            transfer_id=manifest.transfer_id,
                            kind=StatusKind.TRANSFER,
                            state=TransferState.COMPLETE,
                            missing_ranges=[],
                        )
                    ),
                    self._reply_addr_for(manifest, source_addr),
                    reason="short_circuit_status_complete",
                )
            LOGGER.debug(
                "transfer_id=%s short_circuit_existing_complete_file=%s",
                manifest.transfer_id.hex(),
                final_path,
            )
            return
        part_path = self.config.output_dir / f".{manifest.transfer_id.hex()}.part"
        buffered_chunks: list[tuple[int, bytes]] = []
        transfer_to_finalize: _TransferStateData | None = None
        with self._lock:
            existing = self._transfers.get(manifest.transfer_id)
            if existing is not None:
                if (
                    self._manifest_signature(existing.manifest)
                    != self._manifest_signature(manifest)
                ):
                    # Collision policy: ignore conflicting manifest for same transfer ID.
                    return
                existing.source_addr = source_addr
                existing.reply_addr = self._reply_addr_for(manifest, source_addr)
                self._maybe_advertise_receiver_state(sock, existing)
                self._publish_transfer_update_locked(existing, force=True)
                return
            resumed = self._find_transfer_by_manifest_locked(manifest)
            if resumed is not None:
                previous_transfer_id, transfer = resumed
                if previous_transfer_id != manifest.transfer_id:
                    self._remove_transfer_locked(previous_transfer_id)
                    transfer.manifest = manifest
                    self._transfers[manifest.transfer_id] = transfer
                    self._transfer_ids_by_signature[
                        self._manifest_signature(transfer.manifest)
                    ] = manifest.transfer_id
                    self._transfer_ids_by_final_path[transfer.final_path] = manifest.transfer_id
                    self._mark_journal_dirty_locked()
                    LOGGER.debug(
                        (
                            "transfer_id=%s resumed_from_transfer_id=%s "
                            "file=%s received=%d/%d"
                        ),
                        manifest.transfer_id.hex(),
                        previous_transfer_id.hex(),
                        manifest.file_name,
                        transfer.tracker.received_count(),
                        manifest.total_chunks,
                    )
                transfer.source_addr = source_addr
                transfer.reply_addr = self._reply_addr_for(manifest, source_addr)
                self._maybe_advertise_receiver_state(sock, transfer)
                self._publish_transfer_update_locked(transfer, force=True)
                return
            existing_final_owner = self._transfer_ids_by_final_path.get(final_path)
            if existing_final_owner is not None and existing_final_owner != manifest.transfer_id:
                LOGGER.warning(
                    "transfer_id=%s rejected_conflicting_final_path=%s owner=%s",
                    manifest.transfer_id.hex(),
                    final_path,
                    existing_final_owner.hex(),
                )
                return
            if not self._admit_new_transfer_locked(manifest):
                return
            if not part_path.exists():
                with part_path.open("wb") as stream:
                    stream.truncate(manifest.file_size)
            reply = self._reply_addr_for(manifest, source_addr)
            self._transfers[manifest.transfer_id] = _TransferStateData(
                manifest=manifest,
                part_path=part_path,
                final_path=final_path,
                tracker=ChunkTracker(total_chunks=manifest.total_chunks),
                source_addr=source_addr,
                reply_addr=reply,
                last_activity_s=time.monotonic(),
            )
            self._transfer_ids_by_signature[self._manifest_signature(manifest)] = (
                manifest.transfer_id
            )
            self._transfer_ids_by_final_path[final_path] = manifest.transfer_id
            self._active_allocation_bytes += manifest.file_size
            self._ensure_mapped_file_locked(self._transfers[manifest.transfer_id])
            buffered_chunks = self._pop_pre_metadata_buffer_locked(manifest.transfer_id)
            if manifest.total_chunks == 0:
                transfer_to_finalize = self._transfers[manifest.transfer_id]
            self._mark_journal_dirty_locked()
            self._publish_transfer_update_locked(
                self._transfers[manifest.transfer_id],
                force=True,
            )
        LOGGER.debug(
            "transfer prepared transfer_id=%s file=%s chunks=%d source=%s:%d",
            manifest.transfer_id.hex(),
            manifest.file_name,
            manifest.total_chunks,
            source_addr[0],
            source_addr[1],
        )
        if transfer_to_finalize is not None:
            # Empty files have no DATA frames and should complete as soon as
            # metadata is accepted instead of waiting for inactivity timeout.
            self._queue_finalization(sock, transfer_to_finalize, [])
            return
        if buffered_chunks:
            LOGGER.debug(
                "transfer_id=%s replaying_pre_metadata_chunks count=%d",
                manifest.transfer_id.hex(),
                len(buffered_chunks),
            )
            for chunk_index, payload in buffered_chunks:
                self._accept_data(
                    sock,
                    manifest.transfer_id,
                    chunk_index,
                    payload,
                    source_addr=source_addr,
                )
            with self._lock:
                current = self._transfers.get(manifest.transfer_id)
                if current is not None:
                    self._maybe_advertise_receiver_state(sock, current)

    def _maybe_handle_file_info_query(
        self,
        sock: socket.socket,
        manifest: TransferManifest,
        source_addr: tuple[str, int],
    ) -> bool:
        path_raw = manifest.metadata.get(int(MetadataType.FILE_INFO_QUERY_PATH))
        if path_raw is None:
            return False
        if manifest.file_name != "__status_query__":
            return False
        include_checksum_raw = manifest.metadata.get(
            int(MetadataType.FILE_INFO_QUERY_INCLUDE_CHECKSUM)
        )
        query_token = manifest.metadata.get(int(MetadataType.FILE_INFO_QUERY_TOKEN))
        try:
            remote_path = path_raw.decode("utf-8")
        except UnicodeDecodeError:
            return True
        include_checksum = bool(
            include_checksum_raw is not None
            and len(include_checksum_raw) > 0
            and include_checksum_raw[0]
        )
        info = self._query_local_file(
            remote_path=remote_path,
            include_checksum=include_checksum,
        )
        self._sendto_best_effort(
            sock,
            encode_status(
                TransferStatus(
                    transfer_id=manifest.transfer_id,
                    kind=StatusKind.FILE_INFO_RESPONSE,
                    state=TransferState.COMPLETE if info.exists else TransferState.INCOMPLETE,
                    missing_ranges=[],
                    file_info=info,
                    query_token=query_token,
                )
            ),
            source_addr,
            reason="status_file_info_response",
        )
        return True

    def _maybe_advertise_receiver_state(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
    ) -> None:
        if not self.config.enable_feedback:
            return
        if transfer.tracker.received_count() <= 0:
            return
        now = time.monotonic()
        if (
            transfer.last_data_s > 0
            and now - transfer.last_data_s < self.config.forward_stream_quiet_s
            and transfer.tracker.received_count() < transfer.manifest.total_chunks
        ):
            return
        missing_ranges = transfer.tracker.missing_ranges()
        requestable_ranges = self._limit_missing_ranges(missing_ranges)
        state = (
            TransferState.COMPLETE
            if transfer.done and not missing_ranges
            else TransferState.INCOMPLETE
        )
        self._sendto_best_effort(
            sock,
            encode_status(
                TransferStatus(
                    transfer_id=transfer.manifest.transfer_id,
                    kind=StatusKind.TRANSFER,
                    state=state,
                    missing_ranges=requestable_ranges,
                )
            ),
            self._effective_reply_addr(transfer),
            reason="advertise_receiver_state",
        )
        LOGGER.debug(
            "transfer_id=%s advertised_receiver_state missing=%s",
            transfer.manifest.transfer_id.hex(),
            summarize_ranges(requestable_ranges),
        )

    def _find_transfer_by_manifest_locked(
        self,
        manifest: TransferManifest,
    ) -> tuple[bytes, _TransferStateData] | None:
        signature = self._manifest_signature(manifest)
        transfer_id = self._transfer_ids_by_signature.get(signature)
        if transfer_id is None:
            return None
        transfer = self._transfers.get(transfer_id)
        if transfer is None:
            self._transfer_ids_by_signature.pop(signature, None)
            return None
        return transfer_id, transfer

    def _is_matching_completed_file(self, final_path: Path, manifest: TransferManifest) -> bool:
        if not final_path.exists() or not final_path.is_file():
            self._completed_hash_cache.pop(final_path, None)
            return False
        try:
            stat_result = final_path.stat()
            if stat_result.st_size != manifest.file_size:
                return False
            cached = self._completed_hash_cache.get(final_path)
            if cached is not None:
                cached_size, cached_mtime_ns, cached_sha = cached
                if (
                    cached_size == stat_result.st_size
                    and cached_mtime_ns == stat_result.st_mtime_ns
                    and cached_sha == manifest.sha256
                ):
                    return True
            digest = hashlib.sha256()
            with final_path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.digest() != manifest.sha256:
                return False
            self._completed_hash_cache[final_path] = (
                stat_result.st_size,
                stat_result.st_mtime_ns,
                manifest.sha256,
            )
            self._trim_completed_hash_cache()
            return True
        except OSError:
            return False

    def _safe_destination_path(self, file_name: str) -> Path | None:
        return safe_output_path(self.config.output_dir, file_name)

    def _admit_manifest(self, manifest: TransferManifest) -> bool:
        max_file_size = self.config.max_file_size_bytes
        if max_file_size > 0 and manifest.file_size > max_file_size:
            LOGGER.warning(
                "transfer_id=%s rejected_oversize_file size=%d max=%d",
                manifest.transfer_id.hex(),
                manifest.file_size,
                max_file_size,
            )
            return False
        return True

    def _admit_new_transfer_locked(self, manifest: TransferManifest) -> bool:
        max_active = self.config.max_active_transfers
        if max_active > 0 and len(self._transfers) >= max_active:
            LOGGER.warning(
                "transfer_id=%s rejected_active_transfer_limit active=%d max=%d",
                manifest.transfer_id.hex(),
                len(self._transfers),
                max_active,
            )
            return False
        max_allocation = self.config.max_active_allocation_bytes
        projected = self._active_allocation_bytes + manifest.file_size
        if max_allocation > 0 and projected > max_allocation:
            LOGGER.warning(
                "transfer_id=%s rejected_active_allocation projected=%d max=%d",
                manifest.transfer_id.hex(),
                projected,
                max_allocation,
            )
            return False
        return True

    @staticmethod
    def _reply_addr_for(
        manifest: TransferManifest,
        source_addr: tuple[str, int],
    ) -> tuple[str, int]:
        raw = manifest.metadata.get(int(MetadataType.REPLY_PORT))
        if raw is not None and len(raw) == 2:
            port = int.from_bytes(raw, "big")
            if port > 0:
                return (source_addr[0], port)
        return source_addr

    @staticmethod
    def _effective_reply_addr(transfer: _TransferStateData) -> tuple[str, int]:
        return transfer.reply_addr or transfer.source_addr

    def _accept_data(
        self,
        sock: socket.socket,
        transfer_id: bytes,
        chunk_index: int,
        payload: bytes,
        source_addr: tuple[str, int] | None = None,
    ) -> None:
        finalize_transfer: _TransferStateData | None = None
        finalize_missing_ranges: list[tuple[int, int]] | None = None
        with self._lock:
            transfer = self._transfers.get(transfer_id)
            if transfer is None:
                self._buffer_pre_metadata_chunk_locked(transfer_id, chunk_index, payload)
                return
            if transfer.done or transfer.finalized:
                return
            if chunk_index >= transfer.manifest.total_chunks:
                return
            chunk_start = chunk_index * transfer.manifest.chunk_size
            if chunk_start + len(payload) > transfer.manifest.file_size:
                return
            if transfer.mapped_file is not None:
                transfer.mapped_file[chunk_start : chunk_start + len(payload)] = payload
                transfer.mmap_dirty = True
            else:
                with transfer.part_path.open("r+b") as stream:
                    stream.seek(chunk_start)
                    stream.write(payload)
            changed = transfer.tracker.add(chunk_index)
            if changed and chunk_index < transfer.highest_chunk_seen:
                transfer.backfill_chunks += 1
            now_data = time.monotonic()
            transfer.last_activity_s = now_data
            transfer.last_data_s = now_data
            if chunk_index > transfer.highest_chunk_seen:
                transfer.highest_chunk_seen = chunk_index
            transfer.last_chunk_seen = chunk_index
            # Keep receiver->sender beacons alive even under sustained inbound
            # traffic; relying only on timeout ticks can starve beacon sends.
            self._maybe_send_beacon(sock, transfer)
            if chunk_index > 0 and chunk_index % 4096 == 0:
                LOGGER.debug(
                    "transfer_id=%s receiver_chunk_progress=%d/%d",
                    transfer_id.hex(),
                    chunk_index + 1,
                    transfer.manifest.total_chunks,
                )
            if transfer.manifest.total_chunks > 0 and (
                transfer.tracker.received_count() >= transfer.manifest.total_chunks
            ):
                finalize_transfer = transfer
                finalize_missing_ranges = []
            if changed:
                self._mark_journal_dirty_locked()
                self._publish_transfer_update_locked(transfer, force=False)
        if finalize_transfer is not None and finalize_missing_ranges is not None:
            self._queue_finalization(sock, finalize_transfer, finalize_missing_ranges)


    def _send_repair_request(
        self,
        sock: socket.socket | None,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
        *,
        periodic: bool,
    ) -> None:
        if not self.config.enable_feedback:
            return
        now = time.monotonic()
        limited_missing_ranges = self._limit_missing_ranges_for_transfer(
            transfer,
            missing_ranges,
        )
        requested_chunks = sum(end - start for start, end in limited_missing_ranges)
        if requested_chunks <= 0:
            return
        self._sendto_best_effort(
            sock,
            encode_status(
                TransferStatus(
                    transfer_id=transfer.manifest.transfer_id,
                    kind=StatusKind.TRANSFER,
                    state=TransferState.INCOMPLETE,
                    missing_ranges=limited_missing_ranges,
                )
            ),
            self._effective_reply_addr(transfer),
            reason="repair_status_incomplete",
        )
        transfer_id_hex = transfer.manifest.transfer_id.hex()
        transfer.repair_request_in_flight = True
        transfer.received_count_at_last_request = transfer.tracker.received_count()
        transfer.requested_chunks_at_last_request = requested_chunks
        LOGGER.debug(
            (
                "transfer_id=%s sent_%s_repair_request missing=%s "
                "requested_received=%d/%d"
            ),
            transfer_id_hex,
            "periodic" if periodic else "on_demand",
            summarize_ranges(missing_ranges),
            transfer.received_count_at_last_request,
            transfer.manifest.total_chunks,
        )
        transfer.last_periodic_repair_request_s = now

    def _can_send_repair_request(
        self,
        transfer: _TransferStateData,
        now: float,
    ) -> bool:
        if transfer.repair_request_in_flight:
            received_since_request = (
                transfer.tracker.received_count() - transfer.received_count_at_last_request
            )
            if (
                transfer.requested_chunks_at_last_request > 0
                and received_since_request >= transfer.requested_chunks_at_last_request
            ):
                transfer.repair_request_in_flight = False
                # Allow immediate follow-on request after an in-flight batch is fulfilled.
                transfer.last_periodic_repair_request_s = 0.0
                LOGGER.debug(
                    "transfer_id=%s repair_request_inflight_satisfied received=%d requested=%d",
                    transfer.manifest.transfer_id.hex(),
                    received_since_request,
                    transfer.requested_chunks_at_last_request,
                )
                return True
            elapsed_s = now - transfer.last_periodic_repair_request_s
            if (
                transfer.requested_chunks_at_last_request > 0
                and received_since_request > 0
                and elapsed_s >= self.config.periodic_repair_request_s
                and (
                    received_since_request / float(transfer.requested_chunks_at_last_request)
                    >= _REPAIR_PROGRESS_EARLY_RELEASE_RATIO
                )
            ):
                transfer.repair_request_in_flight = False
                transfer.last_periodic_repair_request_s = 0.0
                LOGGER.debug(
                    (
                        "transfer_id=%s repair_request_inflight_progress_release "
                        "received=%d requested=%d elapsed=%.3fs"
                    ),
                    transfer.manifest.transfer_id.hex(),
                    received_since_request,
                    transfer.requested_chunks_at_last_request,
                    elapsed_s,
                )
                return True
            if (
                now - transfer.last_periodic_repair_request_s
                < self.config.repair_request_inflight_timeout_s
            ):
                return False
            transfer.repair_request_in_flight = False
            LOGGER.debug(
                "transfer_id=%s repair_request_inflight_timeout resending",
                transfer.manifest.transfer_id.hex(),
            )
            return True
        if (
            transfer.last_periodic_repair_request_s > 0
            and now - transfer.last_periodic_repair_request_s
            < self.config.repair_request_cooldown_s
        ):
            return False
        return True

    def _maybe_send_periodic_repair_request(
        self,
        sock: socket.socket | None,
        transfer: _TransferStateData,
    ) -> None:
        if not self.config.enable_feedback:
            return
        if self.config.periodic_repair_request_s <= 0:
            return
        if transfer.highest_chunk_seen + 1 < self.config.periodic_repair_min_seen_chunks:
            return
        now = time.monotonic()
        if not self._can_send_repair_request(transfer, now):
            return
        if (
            transfer.last_periodic_repair_request_s > 0
            and now - transfer.last_periodic_repair_request_s
            < self.config.periodic_repair_request_s
        ):
            return
        missing_ranges = transfer.tracker.missing_ranges()
        if not missing_ranges:
            return
        self._send_repair_request(sock, transfer, missing_ranges, periodic=True)

    def _tick_periodic_repairs(self, sock: socket.socket) -> None:
        with self._lock:
            active_transfers = list(self._transfers.values())
        for transfer in active_transfers:
            with self._lock:
                if transfer.done or transfer.finalized:
                    continue
                self._maybe_send_beacon(sock, transfer)
                if self.config.enable_feedback and self.config.periodic_repair_request_s > 0:
                    self._maybe_send_periodic_repair_request(sock, transfer)
                self._flush_transfer_mmap(transfer, force=False)
            self._maybe_finalize_stale_transfer(sock, transfer)

    def _maybe_send_beacon(self, sock: socket.socket, transfer: _TransferStateData) -> None:
        if self.config.beacon_interval_s <= 0:
            return
        now = time.monotonic()
        if (
            transfer.last_beacon_s > 0
            and now - transfer.last_beacon_s < self.config.beacon_interval_s
        ):
            return
        if transfer.last_beacon_rx_s > 0:
            age = int((now - transfer.last_beacon_rx_s) * 1000)
            peer_age = min(age, BEACON_PEER_AGE_NEVER - 1)
        else:
            peer_age = BEACON_PEER_AGE_NEVER
        sent = self._sendto_best_effort(
            sock,
            encode_beacon(
                BeaconRole.RECEIVER, transfer.manifest.transfer_id, peer_age,
            ),
            self._effective_reply_addr(transfer),
            reason="receiver_beacon",
        )
        if sent:
            transfer.last_beacon_s = now
            self._mark_journal_dirty_locked()
            self._publish_beacon_event_locked(
                transfer,
                direction="tx",
                timestamp_s=now,
            )

    def _ensure_mapped_file_locked(self, transfer: _TransferStateData) -> None:
        if transfer.mapped_file is not None:
            return
        if transfer.manifest.file_size <= 0:
            return
        stream: BinaryIO | None = None
        try:
            stream = transfer.part_path.open("r+b")
            transfer.mapped_file = mmap.mmap(stream.fileno(), transfer.manifest.file_size)
            transfer.mapped_stream = stream
            transfer.last_mmap_flush_s = time.monotonic()
            LOGGER.debug(
                "transfer_id=%s mmap_enabled size=%d",
                transfer.manifest.transfer_id.hex(),
                transfer.manifest.file_size,
            )
        except OSError:
            LOGGER.warning(
                "transfer_id=%s failed to create mmap; falling back to direct writes",
                transfer.manifest.transfer_id.hex(),
            )
            if stream is not None:
                stream.close()

    def _flush_transfer_mmap(self, transfer: _TransferStateData, *, force: bool) -> None:
        if transfer.mapped_file is None or not transfer.mmap_dirty:
            return
        now = time.monotonic()
        if (
            not force
            and self.config.journal_flush_interval_s > 0
            and now - transfer.last_mmap_flush_s < self.config.journal_flush_interval_s
        ):
            return
        transfer.mapped_file.flush()
        transfer.mmap_dirty = False
        transfer.last_mmap_flush_s = now

    def _close_transfer_mmap(self, transfer: _TransferStateData) -> None:
        self._flush_transfer_mmap(transfer, force=True)
        if transfer.mapped_file is not None:
            transfer.mapped_file.close()
            transfer.mapped_file = None
        if transfer.mapped_stream is not None:
            transfer.mapped_stream.close()
            transfer.mapped_stream = None

    def _limit_missing_ranges(self, missing_ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
        limit = self.config.max_repair_chunks_per_request
        if limit <= 0:
            return missing_ranges
        return limit_ranges_to_chunk_budget(missing_ranges, limit)

    def _effective_repair_chunk_budget(
        self,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
    ) -> int:
        base_budget = self.config.max_repair_chunks_per_request
        if base_budget <= 0:
            return 0
        if not self.config.adaptive_leading_hole_boost:
            return base_budget
        if not self._is_leading_hole_heavy(transfer, missing_ranges):
            return base_budget
        multiplier = max(1, self.config.leading_hole_boost_multiplier)
        boosted_budget = base_budget * multiplier
        if self.config.leading_hole_max_repair_chunks_per_request > 0:
            boosted_budget = min(
                boosted_budget,
                self.config.leading_hole_max_repair_chunks_per_request,
            )
        return max(base_budget, boosted_budget)

    def _is_leading_hole_heavy(
        self,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
    ) -> bool:
        if not missing_ranges:
            return False
        first_start, first_end = missing_ranges[0]
        if first_start > self.config.leading_hole_start_threshold_chunks:
            return False
        span = max(0, first_end - first_start)
        if span < self.config.leading_hole_min_span_chunks:
            return False
        if transfer.highest_chunk_seen + 1 <= first_start:
            return False
        return True

    def _limit_missing_ranges_for_transfer(
        self,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
    ) -> list[tuple[int, int]]:
        if not missing_ranges:
            return []
        budget = self._effective_repair_chunk_budget(transfer, missing_ranges)
        if budget <= 0:
            return missing_ranges
        if self._is_leading_hole_heavy(transfer, missing_ranges):
            start, end = missing_ranges[0]
            return [(start, min(end, start + budget))]
        return limit_ranges_to_chunk_budget(missing_ranges, budget)

    def _maybe_finalize_stale_transfer(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
    ) -> None:
        with self._lock:
            if transfer.finalized:
                return
            if self.config.transfer_inactivity_timeout_s <= 0:
                return
            inactivity_s = time.monotonic() - transfer.last_activity_s
            if inactivity_s < self.config.transfer_inactivity_timeout_s:
                return
            missing_ranges = transfer.tracker.missing_ranges()
            transfer_id_hex = transfer.manifest.transfer_id.hex()
            if missing_ranges:
                # Keep stale incomplete transfers resumable and visible in the
                # active journal instead of finalizing/removing them.
                transfer.last_activity_s = time.monotonic()
                transfer.repair_request_in_flight = False
                transfer.requested_chunks_at_last_request = 0
                self._mark_journal_dirty_locked()
                LOGGER.warning(
                    (
                        "transfer stale transfer_id=%s inactivity=%.2fs "
                        "retaining incomplete transfer for resume missing=%s"
                    ),
                    transfer_id_hex,
                    inactivity_s,
                    summarize_ranges(missing_ranges),
                )
                return
        LOGGER.warning(
            "transfer stale transfer_id=%s inactivity=%.2fs finalizing missing=%s",
            transfer_id_hex,
            inactivity_s,
            summarize_ranges(missing_ranges),
        )
        self._queue_finalization(sock, transfer, missing_ranges)

    def _run_finalize_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                sock, transfer, missing_ranges = self._finalize_queue.get(timeout=0.5)
            except queue_mod.Empty:
                continue
            self._finalize_transfer_impl(sock, transfer, missing_ranges)

    def _queue_finalization(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
    ) -> None:
        with self._lock:
            if transfer.finalized:
                return
            transfer.finalized = True
            self._close_transfer_mmap(transfer)
            self._mark_journal_dirty_locked()
            self._flush_journal_locked(force=True)
        if self._finalize_thread is not None and self._finalize_thread.is_alive():
            self._finalize_queue.put((sock, transfer, missing_ranges))
        else:
            self._finalize_transfer_impl(sock, transfer, missing_ranges)

    def _finalize_transfer_impl(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
    ) -> None:
        with self._lock:
            manifest = transfer.manifest
            transfer_id = manifest.transfer_id
            transfer_id_hex = transfer_id.hex()
            source_addr = self._effective_reply_addr(transfer)
            part_path = transfer.part_path
            final_path = transfer.final_path
            expected_sha256 = manifest.sha256
            source_mtime = manifest.metadata.get(int(MetadataType.SOURCE_MTIME_NS))
            file_name = manifest.file_name
            status_repeat = self.config.status_repeat
            feedback_enabled = self.config.enable_feedback
            keep_part_files = self.config.keep_part_files_on_complete
        state = TransferState.INCOMPLETE
        hash_mismatch = False
        cache_entry: tuple[int, int, bytes] | None = None
        if not missing_ranges:
            actual_hash = self._stream_file_sha256(part_path)
            if actual_hash == expected_sha256:
                with self._lock:
                    current_owner = self._transfer_ids_by_final_path.get(final_path)
                if current_owner != transfer_id:
                    LOGGER.warning(
                        "transfer_id=%s final_path_owner_changed path=%s owner=%s",
                        transfer_id_hex,
                        final_path,
                        current_owner.hex() if current_owner is not None else "none",
                    )
                    state = TransferState.INCOMPLETE
                else:
                    state = TransferState.COMPLETE
                    if keep_part_files:
                        temp_final_path = (
                            final_path.parent
                            / (
                                f".{final_path.name}."
                                f"{transfer_id_hex}.tmp"
                            )
                        )
                        try:
                            os.link(part_path, temp_final_path)
                        except OSError:
                            shutil.copyfile(part_path, temp_final_path)
                        temp_final_path.replace(final_path)
                    else:
                        part_path.replace(final_path)
                    if source_mtime is not None and len(source_mtime) == 8:
                        mtime_ns = int.from_bytes(source_mtime, "big")
                        os.utime(final_path, ns=(mtime_ns, mtime_ns))
                    try:
                        updated_stat = final_path.stat()
                        cache_entry = (
                            updated_stat.st_size,
                            updated_stat.st_mtime_ns,
                            expected_sha256,
                        )
                    except OSError:
                        cache_entry = None
            else:
                hash_mismatch = True
                state = TransferState.HASH_MISMATCH
                try:
                    part_path.unlink(missing_ok=True)
                except OSError:
                    LOGGER.warning(
                        "transfer_id=%s failed_to_remove_hash_mismatch_part=%s",
                        transfer_id_hex,
                        part_path,
                    )
        with self._lock:
            transfer.done = state == TransferState.COMPLETE
            if cache_entry is not None:
                self._completed_hash_cache[final_path] = cache_entry
                self._trim_completed_hash_cache()
            elif state != TransferState.COMPLETE:
                self._completed_hash_cache.pop(final_path, None)
            info = ReceivedTransferInfo(
                transfer_id_hex=transfer_id_hex,
                file_name=file_name,
                completed=(state == TransferState.COMPLETE),
                missing_ranges=missing_ranges,
                hash_mismatch=hash_mismatch,
            )
            self._completed.append(info)
            self._remove_transfer_locked(transfer_id)
            self._mark_journal_dirty_locked()
            self._flush_journal_locked(force=True)
        LOGGER.info(
            "transfer finalize transfer_id=%s state=%s missing=%s hash_mismatch=%s",
            transfer_id_hex,
            state.name,
            summarize_ranges(missing_ranges),
            hash_mismatch,
        )
        self._publish_transfer_terminal_event(
            transfer_id_hex=transfer_id_hex,
            state=state.name,
            missing_ranges=missing_ranges,
        )
        if feedback_enabled:
            for _ in range(status_repeat):
                self._sendto_best_effort(
                    sock,
                    encode_status(
                        TransferStatus(
                            transfer_id=transfer_id,
                            kind=StatusKind.TRANSFER,
                            state=state,
                            missing_ranges=missing_ranges,
                        )
                    ),
                    source_addr,
                    reason="final_status",
                )

    def _buffer_pre_metadata_chunk_locked(
        self,
        transfer_id: bytes,
        chunk_index: int,
        payload: bytes,
    ) -> None:
        if self.config.pre_metadata_max_pending_bytes <= 0:
            return
        if self.config.pre_metadata_max_pending_bytes_per_transfer <= 0:
            return
        if chunk_index < 0:
            return
        if len(payload) > self.config.pre_metadata_max_pending_bytes_per_transfer:
            return
        pending = self._pending_pre_metadata.get(transfer_id)
        now = time.monotonic()
        if pending is None:
            self._evict_expired_pre_metadata_locked()
            while (
                len(self._pending_pre_metadata)
                >= self.config.pre_metadata_max_pending_transfers
                and self._pending_pre_metadata
            ):
                self._evict_oldest_pre_metadata_locked()
            pending = _PendingPreMetadata(chunks={}, last_update_s=now)
            self._pending_pre_metadata[transfer_id] = pending
        previous = pending.chunks.get(chunk_index)
        projected_transfer_bytes = pending.buffered_bytes + len(payload) - (
            len(previous) if previous is not None else 0
        )
        if projected_transfer_bytes > self.config.pre_metadata_max_pending_bytes_per_transfer:
            return
        projected_total = self._pending_pre_metadata_total_bytes + len(payload) - (
            len(previous) if previous is not None else 0
        )
        while (
            projected_total > self.config.pre_metadata_max_pending_bytes
            and self._pending_pre_metadata
        ):
            if len(self._pending_pre_metadata) == 1 and transfer_id in self._pending_pre_metadata:
                return
            self._evict_oldest_pre_metadata_locked()
            projected_total = self._pending_pre_metadata_total_bytes + len(payload) - (
                len(previous) if previous is not None else 0
            )
        pending.chunks[chunk_index] = payload
        pending.buffered_bytes = projected_transfer_bytes
        pending.last_update_s = now
        self._pending_pre_metadata_total_bytes = projected_total

    def _pop_pre_metadata_buffer_locked(
        self,
        transfer_id: bytes,
    ) -> list[tuple[int, bytes]]:
        pending = self._pending_pre_metadata.pop(transfer_id, None)
        if pending is None:
            return []
        self._pending_pre_metadata_total_bytes = max(
            0,
            self._pending_pre_metadata_total_bytes - pending.buffered_bytes,
        )
        chunks = sorted(pending.chunks.items(), key=lambda item: item[0])
        return chunks

    def _evict_oldest_pre_metadata_locked(self) -> None:
        if not self._pending_pre_metadata:
            return
        oldest_transfer_id = min(
            self._pending_pre_metadata.items(),
            key=lambda item: item[1].last_update_s,
        )[0]
        evicted = self._pending_pre_metadata.pop(oldest_transfer_id)
        self._pending_pre_metadata_total_bytes = max(
            0,
            self._pending_pre_metadata_total_bytes - evicted.buffered_bytes,
        )

    def _evict_expired_pre_metadata_locked(self) -> None:
        if self.config.pre_metadata_ttl_s <= 0:
            return
        now = time.monotonic()
        expired = [
            transfer_id
            for transfer_id, pending in self._pending_pre_metadata.items()
            if now - pending.last_update_s >= self.config.pre_metadata_ttl_s
        ]
        for transfer_id in expired:
            evicted = self._pending_pre_metadata.pop(transfer_id, None)
            if evicted is None:
                continue
            self._pending_pre_metadata_total_bytes = max(
                0,
                self._pending_pre_metadata_total_bytes - evicted.buffered_bytes,
            )

    def _journal_path(self) -> Path:
        return self.config.output_dir / ".ssync-journal.json"

    def _monitor_ipc_socket_path(self) -> Path | None:
        return self.config.monitor_ipc_socket

    def _cleanup_stale_monitor_ipc_socket_path(self, ipc_path: Path) -> None:
        try:
            stat_result = ipc_path.lstat()
        except OSError:
            return
        if not stat.S_ISSOCK(stat_result.st_mode):
            return
        try:
            ipc_path.unlink()
        except OSError:
            return
        LOGGER.debug("removed_stale_monitor_ipc_socket path=%s", ipc_path)

    def _ensure_monitor_ipc_sender(self) -> socket.socket | None:
        if self._monitor_ipc_send_sock is not None:
            return self._monitor_ipc_send_sock
        ipc_path = self._monitor_ipc_socket_path()
        if ipc_path is None:
            return None
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.setblocking(False)
        self._monitor_ipc_send_sock = sock
        return sock

    def _publish_monitor_event(self, payload: dict[str, object]) -> None:
        ipc_path = self._monitor_ipc_socket_path()
        if ipc_path is None:
            return
        sock = self._ensure_monitor_ipc_sender()
        if sock is None:
            return
        try:
            sock.sendto(json.dumps(payload, separators=(",", ":")).encode("utf-8"), str(ipc_path))
        except BlockingIOError:
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            # If monitor crashed and left a stale datagram socket path behind,
            # remove it so a restarted monitor can bind cleanly.
            if exc.errno in {errno.ECONNREFUSED, errno.ENOTCONN}:
                self._cleanup_stale_monitor_ipc_socket_path(ipc_path)
            if self._monitor_ipc_send_sock is not None:
                try:
                    self._monitor_ipc_send_sock.close()
                except OSError:
                    pass
                self._monitor_ipc_send_sock = None
            return

    def _publish_transfer_update_locked(self, transfer: _TransferStateData, *, force: bool) -> None:
        now = time.monotonic()
        if (
            not force
            and transfer.last_monitor_publish_s > 0
            and now - transfer.last_monitor_publish_s < _MONITOR_IPC_MIN_UPDATE_INTERVAL_S
        ):
            return
        transfer.last_monitor_publish_s = now
        self._publish_monitor_event(
            {
                "type": "transfer_update",
                "transfer_id_hex": transfer.manifest.transfer_id.hex(),
                "file_name": transfer.manifest.file_name,
                "file_size": transfer.manifest.file_size,
                "chunk_size": transfer.manifest.chunk_size,
                "total_chunks": transfer.manifest.total_chunks,
                "received_chunks": transfer.tracker.received_count(),
                "range_count": len(transfer.tracker.received_ranges()),
                "stream_cursor_chunk": max(transfer.last_chunk_seen, transfer.highest_chunk_seen),
                "last_beacon_tx_s": transfer.last_beacon_s,
                "last_beacon_rx_s": transfer.last_beacon_rx_s,
                "last_sender_peer_age_ms": transfer.last_sender_peer_age_ms,
                "backfill_chunks": transfer.backfill_chunks,
                "ts_s": now,
            }
        )

    def _publish_beacon_event_locked(
        self,
        transfer: _TransferStateData,
        *,
        direction: str,
        timestamp_s: float | None = None,
    ) -> None:
        now = time.monotonic() if timestamp_s is None else timestamp_s
        if direction == "tx":
            transfer.last_beacon_s = now
        elif direction == "rx":
            transfer.last_beacon_rx_s = now
        else:
            return
        self._publish_monitor_event(
            {
                "type": f"beacon_{direction}",
                "transfer_id_hex": transfer.manifest.transfer_id.hex(),
                "ts_s": now,
            }
        )

    def _publish_transfer_terminal_event(
        self,
        *,
        transfer_id_hex: str,
        state: str,
        missing_ranges: list[tuple[int, int]],
    ) -> None:
        self._publish_monitor_event(
            {
                "type": "transfer_terminal",
                "transfer_id_hex": transfer_id_hex,
                "state": state,
                "missing_ranges": [list(item) for item in missing_ranges],
                "ts_s": time.monotonic(),
            }
        )

    def _manifest_signature(self, manifest: TransferManifest) -> tuple[int, int, bytes, str]:
        return (
            manifest.file_size,
            manifest.chunk_size,
            manifest.sha256,
            manifest.file_name,
        )

    def _save_journal_locked(self) -> None:
        records: list[dict[str, object]] = []
        for transfer_id, transfer in self._transfers.items():
            if transfer.finalized:
                continue
            records.append(
                {
                    "transfer_id_hex": transfer_id.hex(),
                    "manifest": {
                        "file_name": transfer.manifest.file_name,
                        "file_size": transfer.manifest.file_size,
                        "chunk_size": transfer.manifest.chunk_size,
                        "total_chunks": transfer.manifest.total_chunks,
                        "sha256_hex": transfer.manifest.sha256.hex(),
                        "metadata": {
                            str(key): value.hex()
                            for key, value in transfer.manifest.metadata.items()
                        },
                    },
                    "part_path": str(transfer.part_path.relative_to(self.config.output_dir)),
                    "final_path": str(transfer.final_path.relative_to(self.config.output_dir)),
                    "received_ranges": [list(item) for item in transfer.tracker.received_ranges()],
                    "highest_chunk_seen": transfer.highest_chunk_seen,
                    "last_chunk_seen": transfer.last_chunk_seen,
                    "source_addr": [transfer.source_addr[0], transfer.source_addr[1]],
                    "reply_addr": (
                        [transfer.reply_addr[0], transfer.reply_addr[1]]
                        if transfer.reply_addr is not None
                        else None
                    ),
                    "last_beacon_tx_s": transfer.last_beacon_s,
                    "last_beacon_rx_s": transfer.last_beacon_rx_s,
                    "last_sender_peer_age_ms": transfer.last_sender_peer_age_ms,
                    "backfill_chunks": transfer.backfill_chunks,
                }
            )
        journal_path = self._journal_path()
        temp_path = journal_path.with_suffix(".json.tmp")
        temp_path.write_text(json.dumps({"transfers": records}, indent=2), encoding="utf-8")
        temp_path.replace(journal_path)

    def _mark_journal_dirty_locked(self) -> None:
        self._journal_dirty = True

    def _flush_journal_locked(self, *, force: bool) -> None:
        if not self._journal_dirty:
            return
        now = time.monotonic()
        if (
            not force
            and self.config.journal_flush_interval_s > 0
            and now - self._last_journal_flush_s < self.config.journal_flush_interval_s
        ):
            return
        self._save_journal_locked()
        self._journal_dirty = False
        self._last_journal_flush_s = now

    def _load_journal(self) -> None:
        journal_path = self._journal_path()
        if not journal_path.exists():
            return
        try:
            raw = json.loads(journal_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        transfers_raw = raw.get("transfers", [])
        if not isinstance(transfers_raw, list):
            return
        with self._lock:
            for item in transfers_raw:
                if not isinstance(item, dict):
                    continue
                transfer = self._restore_transfer(item)
                if transfer is None:
                    continue
                if not self._admit_manifest(transfer.manifest):
                    continue
                if not self._admit_new_transfer_locked(transfer.manifest):
                    continue
                if transfer.final_path in self._transfer_ids_by_final_path:
                    continue
                self._transfers[transfer.manifest.transfer_id] = transfer
                self._transfer_ids_by_signature[
                    self._manifest_signature(transfer.manifest)
                ] = transfer.manifest.transfer_id
                self._transfer_ids_by_final_path[transfer.final_path] = (
                    transfer.manifest.transfer_id
                )
                self._active_allocation_bytes += transfer.manifest.file_size
            self._mark_journal_dirty_locked()
            self._flush_journal_locked(force=True)

    def _trim_completed_hash_cache(self) -> None:
        while len(self._completed_hash_cache) > _COMPLETED_HASH_CACHE_MAX_ENTRIES:
            oldest = next(iter(self._completed_hash_cache), None)
            if oldest is None:
                break
            self._completed_hash_cache.pop(oldest, None)

    def _remove_transfer_locked(self, transfer_id: bytes) -> _TransferStateData | None:
        transfer = self._transfers.pop(transfer_id, None)
        if transfer is None:
            return None
        signature = self._manifest_signature(transfer.manifest)
        indexed_transfer_id = self._transfer_ids_by_signature.get(signature)
        if indexed_transfer_id == transfer_id:
            self._transfer_ids_by_signature.pop(signature, None)
        indexed_final_path_transfer_id = self._transfer_ids_by_final_path.get(
            transfer.final_path
        )
        if indexed_final_path_transfer_id == transfer_id:
            self._transfer_ids_by_final_path.pop(transfer.final_path, None)
        self._active_allocation_bytes = max(
            0,
            self._active_allocation_bytes - transfer.manifest.file_size,
        )
        return transfer

    def _restore_transfer(self, raw: dict[str, object]) -> _TransferStateData | None:
        try:
            transfer_id = bytes.fromhex(str(raw["transfer_id_hex"]))
            manifest_raw = raw["manifest"]
            if not isinstance(manifest_raw, dict):
                return None
            metadata_raw = manifest_raw.get("metadata", {})
            if not isinstance(metadata_raw, dict):
                return None
            metadata = {int(key): bytes.fromhex(str(value)) for key, value in metadata_raw.items()}
            manifest = TransferManifest(
                transfer_id=transfer_id,
                file_name=str(manifest_raw["file_name"]),
                file_size=int(manifest_raw["file_size"]),
                chunk_size=int(manifest_raw["chunk_size"]),
                total_chunks=int(manifest_raw["total_chunks"]),
                sha256=bytes.fromhex(str(manifest_raw["sha256_hex"])),
                metadata=metadata,
            )
            part_relative = Path(str(raw["part_path"]))
            final_relative = Path(str(raw["final_path"]))
            source_addr_raw = raw["source_addr"]
            if not isinstance(source_addr_raw, list) or len(source_addr_raw) != 2:
                return None
            source_addr = (str(source_addr_raw[0]), int(source_addr_raw[1]))
            reply_addr: tuple[str, int] | None = None
            reply_addr_raw = raw.get("reply_addr")
            if isinstance(reply_addr_raw, list) and len(reply_addr_raw) == 2:
                reply_addr = (str(reply_addr_raw[0]), int(reply_addr_raw[1]))
            highest_chunk_seen = self._coerce_optional_int(raw.get("highest_chunk_seen", -1), -1)
            last_chunk_seen = self._coerce_optional_int(
                raw.get("last_chunk_seen", highest_chunk_seen),
                highest_chunk_seen,
            )
            received_ranges_raw = raw.get("received_ranges", [])
            if not isinstance(received_ranges_raw, list):
                return None
            received_ranges: list[tuple[int, int]] = []
            for value in received_ranges_raw:
                if not isinstance(value, list) or len(value) != 2:
                    return None
                received_ranges.append((int(value[0]), int(value[1])))
        except (KeyError, TypeError, ValueError):
            return None

        if len(transfer_id) != TRANSFER_ID_SIZE:
            return None
        if part_relative.is_absolute() or final_relative.is_absolute():
            return None
        if ".." in part_relative.parts or ".." in final_relative.parts:
            return None
        part_path = self.config.output_dir / part_relative
        final_path = self._safe_destination_path(manifest.file_name)
        if final_path is None:
            return None
        if not part_path.exists():
            return None
        if part_path.stat().st_size != manifest.file_size:
            return None
        tracker = ChunkTracker.from_received_ranges(manifest.total_chunks, received_ranges)
        transfer = _TransferStateData(
            manifest=manifest,
            part_path=part_path,
            final_path=final_path,
            tracker=tracker,
            source_addr=source_addr,
            reply_addr=reply_addr or self._reply_addr_for(manifest, source_addr),
            last_activity_s=time.monotonic(),
            highest_chunk_seen=max(-1, highest_chunk_seen),
            last_chunk_seen=max(-1, last_chunk_seen),
            last_beacon_s=self._coerce_optional_float(raw.get("last_beacon_tx_s", 0.0), 0.0),
            last_beacon_rx_s=self._coerce_optional_float(
                raw.get("last_beacon_rx_s", 0.0), 0.0
            ),
        )
        self._ensure_mapped_file_locked(transfer)
        return transfer

    def _query_local_file(self, *, remote_path: str, include_checksum: bool) -> RemoteFileInfo:
        final_path = self._safe_destination_path(remote_path)
        if final_path is None or not final_path.exists() or not final_path.is_file():
            return RemoteFileInfo(path=remote_path, exists=False)
        stat = final_path.stat()
        file_hash = self._stream_file_sha256(final_path) if include_checksum else None
        return RemoteFileInfo(
            path=remote_path,
            exists=True,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            sha256=file_hash,
        )

    @staticmethod
    def _stream_file_sha256(path: Path) -> bytes:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.digest()

    @staticmethod
    def _coerce_optional_int(value: object, default: int) -> int:
        if isinstance(value, bool):
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            return int(value)
        return default

    @staticmethod
    def _coerce_optional_float(value: object, default: float) -> float:
        if isinstance(value, bool):
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value)
        return default

    def _run_repair_timer(self) -> None:
        """Fire periodic repair requests on a wall-clock timer, independent of
        whether the main ``recvfrom()`` loop is busy with sustained inbound
        DATA traffic.  Skips transfers whose forward stream is still active
        to avoid lock contention with the hot ``_accept_data`` path."""
        interval = self.config.periodic_repair_request_s
        quiet = self.config.forward_stream_quiet_s
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=interval)
            if self._stop_event.is_set():
                break
            now = time.monotonic()
            with self._lock:
                active_transfers = list(self._transfers.values())
            for transfer in active_transfers:
                with self._lock:
                    if transfer.done or transfer.finalized:
                        continue
                    if (
                        transfer.last_data_s > 0
                        and now - transfer.last_data_s < quiet
                        and transfer.tracker.received_count()
                        < transfer.manifest.total_chunks
                    ):
                        continue
                    self._maybe_send_periodic_repair_request(None, transfer)

    def _run_send_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                payload, destination = self._send_queue.get(timeout=0.1)
            except queue_mod.Empty:
                continue
            tx = self._tx_sock
            if tx is None:
                continue
            try:
                tx.sendto(payload, destination)
            except OSError:
                pass

    def _sendto_best_effort(
        self,
        sock: socket.socket | None,
        payload: bytes,
        destination: tuple[str, int],
        *,
        reason: str,
    ) -> bool:
        if self._send_thread is not None and self._send_thread.is_alive():
            try:
                self._send_queue.put_nowait((payload, destination))
            except queue_mod.Full:
                LOGGER.debug(
                    "send_queue_full reason=%s dest=%s:%d",
                    reason,
                    destination[0],
                    destination[1],
                )
                return False
            return True
        if sock is None:
            LOGGER.debug(
                "send_socket_unavailable reason=%s dest=%s:%d",
                reason,
                destination[0],
                destination[1],
            )
            return False
        try:
            sock.sendto(payload, destination)
        except OSError as exc:
            LOGGER.debug(
                "sendto_failed reason=%s dest=%s:%d error=%s",
                reason,
                destination[0],
                destination[1],
                exc,
            )
            return False
        return True

