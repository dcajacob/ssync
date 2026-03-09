from __future__ import annotations

import hashlib
import json
import logging
import mmap
import os
import shutil
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from .frames import (
    TransferStatus,
    decode_beacon,
    decode_data_chunk,
    decode_file_info_request,
    decode_fin,
    decode_frame,
    decode_manifest,
    decode_repair_done,
    encode_beacon,
    encode_file_info_response,
    encode_repair_request,
    encode_status,
    encode_transfer_complete,
)
from .manifest import RepairRequest, TransferManifest
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
    TransferState,
)

LOGGER = logging.getLogger(__name__)
@dataclass(slots=True)
class _TransferStateData:
    manifest: TransferManifest
    part_path: Path
    final_path: Path
    tracker: ChunkTracker
    source_addr: tuple[str, int]
    done: bool = False
    finalized: bool = False
    hash_mismatch: bool = False
    fin_received: bool = False
    highest_chunk_seen: int = -1
    last_chunk_seen: int = -1
    last_periodic_repair_request_s: float = 0.0
    last_repair_done_s: float = 0.0
    repair_request_in_flight: bool = False
    received_count_at_last_request: int = 0
    last_activity_s: float = 0.0
    mapped_stream: BinaryIO | None = None
    mapped_file: mmap.mmap | None = None
    mmap_dirty: bool = False
    last_mmap_flush_s: float = 0.0
    last_beacon_s: float = 0.0


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
        self._completed: list[ReceivedTransferInfo] = []
        self._thread: threading.Thread | None = None
        self._maintenance_thread: threading.Thread | None = None
        self._journal_dirty = False
        self._last_journal_flush_s = 0.0
        self._completed_hash_cache: dict[Path, tuple[int, int, bytes]] = {}

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
                "inactivity=%.2fs rcvbuf=%d journal_flush=%.3fs"
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
        )
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        self._maintenance_thread = threading.Thread(
            target=self._run_maintenance_loop,
            daemon=True,
        )
        self._maintenance_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        if self._maintenance_thread:
            self._maintenance_thread.join(timeout=2.0)
        with self._lock:
            for transfer in self._transfers.values():
                self._close_transfer_mmap(transfer)
            self._flush_journal_locked(force=True)
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
        if frame_type == FrameType.MANIFEST:
            try:
                manifest = decode_manifest(payload)
            except ValueError:
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
            )
            return
        if frame_type == FrameType.FIN:
            try:
                transfer_id = decode_fin(payload)
            except ValueError:
                return
            self._on_fin(sock, transfer_id)
            return
        if frame_type == FrameType.REPAIR_DONE:
            try:
                transfer_id = decode_repair_done(payload)
            except ValueError:
                return
            self._on_repair_done(sock, transfer_id)
            return
        if frame_type == FrameType.FILE_INFO_REQUEST:
            try:
                remote_path, include_checksum = decode_file_info_request(payload)
            except ValueError:
                return
            info = self._query_local_file(
                remote_path=remote_path,
                include_checksum=include_checksum,
            )
            self._sendto_best_effort(
                sock,
                encode_file_info_response(info),
                source_addr,
                reason="file_info_response",
            )
            return
        if frame_type == FrameType.BEACON:
            try:
                _role, transfer_id = decode_beacon(payload)
            except ValueError:
                return
            with self._lock:
                transfer = self._transfers.get(transfer_id)
                if transfer is None or transfer.finalized:
                    return
                transfer.source_addr = source_addr
                transfer.last_activity_s = time.monotonic()
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
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if self._is_matching_completed_file(final_path, manifest):
            if self.config.enable_feedback:
                self._sendto_best_effort(
                    sock,
                    encode_status(
                        TransferStatus(
                            transfer_id=manifest.transfer_id,
                            state=TransferState.COMPLETE,
                            missing_ranges=[],
                        )
                    ),
                    source_addr,
                    reason="short_circuit_status_complete",
                )
            self._sendto_best_effort(
                sock,
                encode_transfer_complete(manifest.transfer_id),
                source_addr,
                reason="short_circuit_transfer_complete",
            )
            LOGGER.debug(
                "transfer_id=%s short_circuit_existing_complete_file=%s",
                manifest.transfer_id.hex(),
                final_path,
            )
            return
        part_path = self.config.output_dir / f".{manifest.transfer_id.hex()}.part"
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
                self._maybe_advertise_receiver_state(sock, existing)
                return
            resumed = self._find_transfer_by_manifest_locked(manifest)
            if resumed is not None:
                previous_transfer_id, transfer = resumed
                if previous_transfer_id != manifest.transfer_id:
                    self._transfers.pop(previous_transfer_id, None)
                    transfer.manifest = manifest
                    self._transfers[manifest.transfer_id] = transfer
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
                self._maybe_advertise_receiver_state(sock, transfer)
                return
            if not part_path.exists():
                with part_path.open("wb") as stream:
                    stream.truncate(manifest.file_size)
            self._transfers[manifest.transfer_id] = _TransferStateData(
                manifest=manifest,
                part_path=part_path,
                final_path=final_path,
                tracker=ChunkTracker(total_chunks=manifest.total_chunks),
                source_addr=source_addr,
                last_activity_s=time.monotonic(),
            )
            self._ensure_mapped_file_locked(self._transfers[manifest.transfer_id])
            self._mark_journal_dirty_locked()
        LOGGER.debug(
            "transfer prepared transfer_id=%s file=%s chunks=%d source=%s:%d",
            manifest.transfer_id.hex(),
            manifest.file_name,
            manifest.total_chunks,
            source_addr[0],
            source_addr[1],
        )

    def _maybe_advertise_receiver_state(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
    ) -> None:
        if not self.config.enable_feedback:
            return
        if transfer.tracker.received_count() <= 0:
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
                    state=state,
                    missing_ranges=requestable_ranges,
                )
            ),
            transfer.source_addr,
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
        for transfer_id, transfer in self._transfers.items():
            if self._manifest_signature(transfer.manifest) == signature:
                return transfer_id, transfer
        return None

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
            return True
        except OSError:
            return False

    def _safe_destination_path(self, file_name: str) -> Path | None:
        relative_path = Path(file_name)
        if relative_path.is_absolute():
            return None
        filtered_parts = [part for part in relative_path.parts if part not in ("", ".")]
        if not filtered_parts:
            return None
        if any(part == ".." for part in filtered_parts):
            return None
        return self.config.output_dir / Path(*filtered_parts)

    def _accept_data(
        self,
        sock: socket.socket,
        transfer_id: bytes,
        chunk_index: int,
        payload: bytes,
    ) -> None:
        with self._lock:
            transfer = self._transfers.get(transfer_id)
            if transfer is None or transfer.done or transfer.finalized:
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
            transfer.last_activity_s = time.monotonic()
            if chunk_index > transfer.highest_chunk_seen:
                transfer.highest_chunk_seen = chunk_index
            transfer.last_chunk_seen = chunk_index
            if chunk_index > 0 and chunk_index % 4096 == 0:
                LOGGER.debug(
                    "transfer_id=%s receiver_chunk_progress=%d/%d",
                    transfer_id.hex(),
                    chunk_index + 1,
                    transfer.manifest.total_chunks,
                )
            self._maybe_send_periodic_repair_request(sock, transfer)
            if changed:
                self._mark_journal_dirty_locked()

    def _on_fin(self, sock: socket.socket, transfer_id: bytes) -> None:
        with self._lock:
            transfer = self._transfers.get(transfer_id)
            if transfer is None or transfer.finalized:
                return
            transfer.fin_received = True
            transfer.last_activity_s = time.monotonic()
            missing_ranges = transfer.tracker.missing_ranges()
            LOGGER.debug(
                "transfer_id=%s received_fin missing=%s",
                transfer_id.hex(),
                summarize_ranges(missing_ranges),
            )
            if missing_ranges and self.config.enable_feedback:
                self._send_repair_request(sock, transfer, missing_ranges, periodic=False)
                return
            self._finalize_transfer(sock, transfer, missing_ranges)

    def _on_repair_done(self, sock: socket.socket, transfer_id: bytes) -> None:
        with self._lock:
            transfer = self._transfers.get(transfer_id)
            if transfer is None or transfer.done or transfer.finalized:
                return
            transfer.last_activity_s = time.monotonic()
            transfer.last_repair_done_s = transfer.last_activity_s
            transfer.repair_request_in_flight = False
            missing_ranges = transfer.tracker.missing_ranges()
            if missing_ranges and self.config.enable_feedback:
                missing_count = sum(end - start for start, end in missing_ranges)
                progress_delta = (
                    transfer.tracker.received_count() - transfer.received_count_at_last_request
                )
                LOGGER.debug(
                    (
                        "transfer_id=%s received_repair_done still_missing=%s "
                        "missing_chunks=%d progress_delta=%d received=%d/%d"
                    ),
                    transfer_id.hex(),
                    summarize_ranges(missing_ranges),
                    missing_count,
                    progress_delta,
                    transfer.tracker.received_count(),
                    transfer.manifest.total_chunks,
                )
                if self._can_send_repair_request(transfer, transfer.last_activity_s):
                    self._send_repair_request(sock, transfer, missing_ranges, periodic=False)
                return
            self._finalize_transfer(sock, transfer, missing_ranges)

    def _send_repair_request(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
        *,
        periodic: bool,
    ) -> None:
        if not self.config.enable_feedback:
            return
        now = time.monotonic()
        request = RepairRequest(
            transfer_id=transfer.manifest.transfer_id,
            missing_ranges=self._limit_missing_ranges(missing_ranges),
        )
        sent = self._sendto_best_effort(
            sock,
            encode_repair_request(request),
            transfer.source_addr,
            reason="repair_request",
        )
        if not sent:
            return
        transfer_id_hex = transfer.manifest.transfer_id.hex()
        transfer.repair_request_in_flight = True
        transfer.received_count_at_last_request = transfer.tracker.received_count()
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
        if transfer.fin_received and transfer.last_repair_done_s > 0:
            if (
                transfer.tracker.received_count()
                <= transfer.received_count_at_last_request
                and now - transfer.last_repair_done_s
                < self.config.repair_request_cooldown_s
            ):
                return False
        return True

    def _maybe_send_periodic_repair_request(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
    ) -> None:
        if not self.config.enable_feedback:
            return
        if self.config.periodic_repair_request_s <= 0:
            return
        if transfer.highest_chunk_seen + 1 < self.config.periodic_repair_min_seen_chunks:
            return
        now = time.monotonic()
        if (
            transfer.last_periodic_repair_request_s > 0
            and now - transfer.last_periodic_repair_request_s
            < self.config.periodic_repair_request_s
        ):
            return
        if not self._can_send_repair_request(transfer, now):
            return
        if transfer.fin_received:
            missing_ranges = transfer.tracker.missing_ranges()
        else:
            missing_ranges = transfer.tracker.missing_ranges_upto(transfer.highest_chunk_seen + 1)
        if not missing_ranges:
            return
        self._send_repair_request(sock, transfer, missing_ranges, periodic=True)

    def _tick_periodic_repairs(self, sock: socket.socket) -> None:
        with self._lock:
            active_transfers = [
                transfer for transfer in self._transfers.values() if not transfer.done
            ]
            for transfer in active_transfers:
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
        sent = self._sendto_best_effort(
            sock,
            encode_beacon(BeaconRole.RECEIVER, transfer.manifest.transfer_id),
            transfer.source_addr,
            reason="receiver_beacon",
        )
        if sent:
            transfer.last_beacon_s = now

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

    def _maybe_finalize_stale_transfer(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
    ) -> None:
        if transfer.finalized:
            return
        if not transfer.fin_received:
            return
        if self.config.transfer_inactivity_timeout_s <= 0:
            return
        if time.monotonic() - transfer.last_activity_s < self.config.transfer_inactivity_timeout_s:
            return
        missing_ranges = transfer.tracker.missing_ranges()
        LOGGER.warning(
            "transfer stale transfer_id=%s inactivity=%.2fs finalizing incomplete missing=%s",
            transfer.manifest.transfer_id.hex(),
            time.monotonic() - transfer.last_activity_s,
            summarize_ranges(missing_ranges),
        )
        self._finalize_transfer(sock, transfer, missing_ranges)

    def _finalize_transfer(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
    ) -> None:
        is_owned = getattr(self._lock, "_is_owned", None)
        assert is_owned is None or bool(is_owned())
        if transfer.finalized:
            return
        transfer.finalized = True
        state = TransferState.INCOMPLETE
        hash_mismatch = False
        self._close_transfer_mmap(transfer)
        if not missing_ranges:
            actual_hash = hashlib.sha256(transfer.part_path.read_bytes()).digest()
            if actual_hash == transfer.manifest.sha256:
                state = TransferState.COMPLETE
                if self.config.keep_part_files_on_complete:
                    temp_final_path = (
                        transfer.final_path.parent
                        / (
                            f".{transfer.final_path.name}."
                            f"{transfer.manifest.transfer_id.hex()}.tmp"
                        )
                    )
                    try:
                        os.link(transfer.part_path, temp_final_path)
                    except OSError:
                        shutil.copyfile(transfer.part_path, temp_final_path)
                    temp_final_path.replace(transfer.final_path)
                else:
                    transfer.part_path.replace(transfer.final_path)
                source_mtime = transfer.manifest.metadata.get(int(MetadataType.SOURCE_MTIME_NS))
                if source_mtime is not None and len(source_mtime) == 8:
                    mtime_ns = int.from_bytes(source_mtime, "big")
                    os.utime(transfer.final_path, ns=(mtime_ns, mtime_ns))
                try:
                    updated_stat = transfer.final_path.stat()
                    self._completed_hash_cache[transfer.final_path] = (
                        updated_stat.st_size,
                        updated_stat.st_mtime_ns,
                        transfer.manifest.sha256,
                    )
                except OSError:
                    self._completed_hash_cache.pop(transfer.final_path, None)
            else:
                hash_mismatch = True
                state = TransferState.HASH_MISMATCH
                try:
                    transfer.part_path.unlink(missing_ok=True)
                except OSError:
                    LOGGER.warning(
                        "transfer_id=%s failed_to_remove_hash_mismatch_part=%s",
                        transfer.manifest.transfer_id.hex(),
                        transfer.part_path,
                    )
        transfer.done = (state == TransferState.COMPLETE)
        LOGGER.info(
            "transfer finalize transfer_id=%s state=%s missing=%s hash_mismatch=%s",
            transfer.manifest.transfer_id.hex(),
            state.name,
            summarize_ranges(missing_ranges),
            hash_mismatch,
        )
        if self.config.enable_feedback:
            for _ in range(self.config.status_repeat):
                self._sendto_best_effort(
                    sock,
                    encode_status(
                        TransferStatus(
                            transfer_id=transfer.manifest.transfer_id,
                            state=state,
                            missing_ranges=missing_ranges,
                        )
                    ),
                    transfer.source_addr,
                    reason="final_status",
                )
            if state == TransferState.COMPLETE:
                for _ in range(self.config.status_repeat):
                    self._sendto_best_effort(
                        sock,
                        encode_transfer_complete(transfer.manifest.transfer_id),
                        transfer.source_addr,
                        reason="final_transfer_complete",
                    )
        info = ReceivedTransferInfo(
            transfer_id_hex=transfer.manifest.transfer_id.hex(),
            file_name=transfer.manifest.file_name,
            completed=(state == TransferState.COMPLETE),
            missing_ranges=missing_ranges,
            hash_mismatch=hash_mismatch,
        )
        self._completed.append(info)
        self._transfers.pop(transfer.manifest.transfer_id, None)
        self._mark_journal_dirty_locked()
        self._flush_journal_locked(force=True)

    def _journal_path(self) -> Path:
        return self.config.output_dir / ".ssync-journal.json"

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
                self._transfers[transfer.manifest.transfer_id] = transfer
            self._mark_journal_dirty_locked()
            self._flush_journal_locked(force=True)

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
            last_activity_s=time.monotonic(),
            highest_chunk_seen=max(-1, highest_chunk_seen),
            last_chunk_seen=max(-1, last_chunk_seen),
        )
        self._ensure_mapped_file_locked(transfer)
        return transfer

    def _query_local_file(self, *, remote_path: str, include_checksum: bool) -> RemoteFileInfo:
        final_path = self._safe_destination_path(remote_path)
        if final_path is None or not final_path.exists() or not final_path.is_file():
            return RemoteFileInfo(path=remote_path, exists=False)
        stat = final_path.stat()
        file_hash = hashlib.sha256(final_path.read_bytes()).digest() if include_checksum else None
        return RemoteFileInfo(
            path=remote_path,
            exists=True,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            sha256=file_hash,
        )

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

    def _sendto_best_effort(
        self,
        sock: socket.socket,
        payload: bytes,
        destination: tuple[str, int],
        *,
        reason: str,
    ) -> bool:
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

