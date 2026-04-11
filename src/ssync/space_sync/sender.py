from __future__ import annotations

import logging
import math
import queue
import socket
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO

from .frames import (
    decode_frame,
    decode_status,
    encode_beacon,
    encode_data_chunk,
    encode_metadata,
)
from .manifest import TransferManifest
from .ranges import expand_ranges, limit_ranges_to_chunk_budget, summarize_ranges
from .types import (
    BeaconRole,
    FrameType,
    MetadataType,
    RemoteFileInfo,
    SenderConfig,
    SendResult,
    StatusKind,
    TransferState,
)

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class _QueuedRepairRequest:
    missing_ranges: list[tuple[int, int]]
    signature: tuple[tuple[int, int], ...]
    enqueued_s: float


@dataclass(slots=True)
class _ParallelRepairRuntime:
    queue: queue.Queue[_QueuedRepairRequest]
    stop_event: threading.Event
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    completed: bool = False
    hash_mismatch: bool = False
    saw_uplink: bool = False
    repaired_chunks: int = 0
    repair_rounds: int = 0
    suppressed_duplicates: int = 0
    last_signature: tuple[tuple[int, int], ...] | None = None
    last_signature_s: float = 0.0


class SpaceSyncSender:
    def __init__(self, config: SenderConfig | None = None) -> None:
        self.config = config or SenderConfig()
        self._auto_feedback_active = False
        self._last_auto_uplink_activity_s: float | None = None

    def send_file(
        self,
        file_path: Path,
        destination_host: str,
        destination_port: int,
        remote_name: str | None = None,
        stop_requested: Callable[[], bool] | None = None,
        transfer_id: bytes | None = None,
        send_initial_data: bool = True,
        max_repair_rounds_override: int | None = None,
        max_feedback_seconds_override: float | None = None,
        max_feedback_total_rounds_override: int | None = None,
        local_sha256_override: bytes | None = None,
    ) -> SendResult:
        file_path = file_path.resolve()
        file_stat = file_path.stat()
        file_size = file_stat.st_size
        total_chunks = math.ceil(file_size / self.config.chunk_size) if file_size else 0
        file_checksum = local_sha256_override
        if file_checksum is None or len(file_checksum) != 32:
            file_checksum = self.local_file_checksum(file_path)
        metadata = {
            int(MetadataType.SOURCE_MTIME_NS): int(file_stat.st_mtime_ns).to_bytes(8, "big"),
        }
        manifest = TransferManifest(
            transfer_id=transfer_id if transfer_id is not None else uuid.uuid4().bytes,
            file_name=remote_name or file_path.name,
            file_size=file_size,
            chunk_size=self.config.chunk_size,
            total_chunks=total_chunks,
            sha256=file_checksum,
            metadata=metadata,
        )
        destination = (destination_host, destination_port)
        repaired_chunks = 0
        repair_rounds = 0
        feedback_budget_rounds = 0
        completed = False
        effective_max_repair_rounds = (
            self.config.max_repair_rounds
            if max_repair_rounds_override is None
            else max_repair_rounds_override
        )
        effective_max_feedback_seconds = (
            self.config.primary_feedback_max_seconds
            if max_feedback_seconds_override is None
            else max(0.0, max_feedback_seconds_override)
        )
        effective_max_feedback_total_rounds = (
            self.config.primary_feedback_max_rounds
            if max_feedback_total_rounds_override is None
            else max(0, max_feedback_total_rounds_override)
        )
        auto_feedback_enabled = self.config.auto_feedback_discovery
        auto_feedback_timeout_s = max(0.0, self.config.auto_feedback_idle_timeout_s)
        feedback_active = self.config.enable_feedback
        if auto_feedback_enabled and not self.config.enable_feedback:
            feedback_active = self._auto_feedback_active
            if (
                feedback_active
                and auto_feedback_timeout_s > 0
                and self._last_auto_uplink_activity_s is not None
                and (time.monotonic() - self._last_auto_uplink_activity_s)
                >= auto_feedback_timeout_s
            ):
                feedback_active = False
        last_uplink_activity_s: float | None = time.monotonic() if feedback_active else None
        paced_start_s = time.monotonic()
        paced_data_bytes = 0
        transfer_id_hex = manifest.transfer_id.hex()
        feedback_deadline_s = (
            (paced_start_s + effective_max_feedback_seconds)
            if (feedback_active and effective_max_feedback_seconds > 0)
            else None
        )
        budget_stop_reason: str | None = None
        prioritize_forward_data = (
            feedback_active
            and send_initial_data
            and (
                effective_max_feedback_seconds > 0
                or effective_max_feedback_total_rounds > 0
            )
        )
        initial_data_phase_complete = not send_initial_data

        def _feedback_budget_exhausted() -> bool:
            nonlocal budget_stop_reason
            if not feedback_active:
                return False
            if not initial_data_phase_complete:
                return False
            if (
                effective_max_feedback_total_rounds > 0
                and feedback_budget_rounds >= effective_max_feedback_total_rounds
            ):
                budget_stop_reason = (
                    "max_feedback_total_rounds="
                    f"{effective_max_feedback_total_rounds}"
                )
                return True
            if feedback_deadline_s is not None and time.monotonic() >= feedback_deadline_s:
                budget_stop_reason = (
                    f"max_feedback_seconds={effective_max_feedback_seconds:.3f}"
                )
                return True
            return False
        LOGGER.info(
            "send start transfer_id=%s file=%s remote=%s chunks=%d feedback=%s auto=%s",
            transfer_id_hex,
            file_path,
            manifest.file_name,
            total_chunks,
            feedback_active,
            auto_feedback_enabled,
        )
        if (
            feedback_active
            and self.config.inter_packet_delay_s <= 0
            and total_chunks > 32768
        ):
            LOGGER.warning(
                "transfer_id=%s zero inter-packet delay on large transfer may cause heavy loss",
                transfer_id_hex,
            )

        with (
            file_path.open("rb") as file_stream,
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock,
        ):
            chunk_reader_lock = threading.Lock()

            def chunk_reader(index: int) -> bytes:
                # Parallel repair sends and primary forward sends can both read
                # from the same file stream. Guard seek/read so offsets cannot
                # interleave across threads and corrupt chunk payload selection.
                with chunk_reader_lock:
                    return self._read_chunk(
                        file_stream=file_stream,
                        chunk_size=self.config.chunk_size,
                        chunk_index=index,
                    )
            if feedback_active:
                sock.settimeout(self.config.feedback_wait_s)
                sock.setblocking(False)
            else:
                sock.setblocking(True)
                # Use finite timeout so Ctrl-C/stop checks can break blocked sends.
                sock.settimeout(0.5)

            def _set_feedback_mode(active: bool, reason: str) -> None:
                nonlocal feedback_active, feedback_deadline_s, prioritize_forward_data
                nonlocal last_uplink_activity_s
                if active == feedback_active:
                    return
                feedback_active = active
                if feedback_active:
                    sock.settimeout(self.config.feedback_wait_s)
                    sock.setblocking(False)
                    last_uplink_activity_s = time.monotonic()
                    if effective_max_feedback_seconds > 0:
                        feedback_deadline_s = (
                            time.monotonic() + effective_max_feedback_seconds
                        )
                    prioritize_forward_data = (
                        send_initial_data
                        and (
                            effective_max_feedback_seconds > 0
                            or effective_max_feedback_total_rounds > 0
                        )
                    )
                else:
                    sock.setblocking(True)
                    sock.settimeout(0.5)
                    feedback_deadline_s = None
                    prioritize_forward_data = False
                LOGGER.info(
                    "transfer_id=%s feedback_mode=%s reason=%s",
                    transfer_id_hex,
                    "enabled" if feedback_active else "disabled",
                    reason,
                )

            def _probe_uplink_packets(max_packets: int = 8) -> bool:
                # Probe opportunistically while running in open-loop socket mode.
                if not hasattr(sock, "recvfrom"):
                    return False
                sock.setblocking(False)
                saw_uplink = False
                for _ in range(max(1, max_packets)):
                    try:
                        _response_raw, _response_addr = sock.recvfrom(65535)
                    except (BlockingIOError, TimeoutError):
                        break
                    saw_uplink = True
                sock.setblocking(True)
                sock.settimeout(0.5)
                return saw_uplink

            parallel_runtime: _ParallelRepairRuntime | None = None
            parallel_threads: list[threading.Thread] = []

            def _start_parallel_repairs() -> None:
                nonlocal parallel_runtime, last_uplink_activity_s, completed
                if parallel_runtime is not None or not feedback_active:
                    return
                parallel_runtime = _ParallelRepairRuntime(
                    queue=queue.Queue(
                        maxsize=max(1, self.config.repair_queue_max_pending_requests)
                    ),
                    stop_event=threading.Event(),
                )

                def _recv_pump() -> None:
                    nonlocal last_uplink_activity_s
                    assert parallel_runtime is not None
                    while not parallel_runtime.stop_event.is_set():
                        try:
                            response_raw, _response_addr = sock.recvfrom(65535)
                        except (BlockingIOError, TimeoutError):
                            time.sleep(self.config.repair_worker_poll_interval_s)
                            continue
                        try:
                            parsed = decode_frame(response_raw)
                        except ValueError:
                            continue
                        if parsed.frame_type is None:
                            continue
                        with parallel_runtime.state_lock:
                            parallel_runtime.saw_uplink = True
                        last_uplink_activity_s = time.monotonic()
                        if parsed.frame_type != FrameType.STATUS:
                            continue
                        status = decode_status(parsed.payload)
                        if status.kind != StatusKind.TRANSFER:
                            continue
                        if status.transfer_id != manifest.transfer_id:
                            continue
                        if status.state == TransferState.COMPLETE:
                            with parallel_runtime.state_lock:
                                parallel_runtime.completed = True
                            parallel_runtime.stop_event.set()
                            return
                        if status.state == TransferState.HASH_MISMATCH:
                            with parallel_runtime.state_lock:
                                parallel_runtime.hash_mismatch = True
                            parallel_runtime.stop_event.set()
                            return
                        if status.state != TransferState.INCOMPLETE or not status.missing_ranges:
                            continue
                        signature = tuple(status.missing_ranges)
                        request = _QueuedRepairRequest(
                            missing_ranges=status.missing_ranges,
                            signature=signature,
                            enqueued_s=time.monotonic(),
                        )
                        try:
                            parallel_runtime.queue.put_nowait(request)
                        except queue.Full:
                            continue

                def _repair_worker() -> None:
                    nonlocal paced_data_bytes
                    assert parallel_runtime is not None
                    while not parallel_runtime.stop_event.is_set():
                        try:
                            request = parallel_runtime.queue.get(
                                timeout=self.config.repair_worker_poll_interval_s
                            )
                        except queue.Empty:
                            continue
                        with parallel_runtime.state_lock:
                            if (
                                self.config.repair_duplicate_suppression_s > 0
                                and parallel_runtime.last_signature == request.signature
                                and request.enqueued_s - parallel_runtime.last_signature_s
                                < self.config.repair_duplicate_suppression_s
                            ):
                                parallel_runtime.suppressed_duplicates += 1
                                continue
                        if (
                            send_initial_data
                            and not initial_data_phase_complete
                            and prioritize_forward_data
                        ):
                            # During the first-pass forward stream, keep only the
                            # freshest repair request to avoid replaying stale
                            # missing snapshots and overwhelming the downlink.
                            while True:
                                try:
                                    request = parallel_runtime.queue.get_nowait()
                                except queue.Empty:
                                    break
                        limited_ranges = limit_ranges_to_chunk_budget(
                            request.missing_ranges,
                            max(
                                1,
                                min(
                                    self.config.repair_worker_max_chunks_per_burst,
                                    self.config.initial_pass_repair_max_chunks_per_burst
                                    if (
                                        send_initial_data
                                        and not initial_data_phase_complete
                                        and prioritize_forward_data
                                    )
                                    else self.config.repair_worker_max_chunks_per_burst,
                                ),
                            ),
                        )
                        repaired_now, _, paced_data_bytes = self._send_requested_repairs(
                            sock=sock,
                            manifest=manifest,
                            total_chunks=total_chunks,
                            chunk_reader=chunk_reader,
                            destination=destination,
                            missing_ranges=limited_ranges,
                            paced_start_s=paced_start_s,
                            paced_data_bytes=paced_data_bytes,
                        )
                        with parallel_runtime.state_lock:
                            if repaired_now > 0:
                                parallel_runtime.repaired_chunks += repaired_now
                                parallel_runtime.repair_rounds += 1
                            parallel_runtime.last_signature = request.signature
                            parallel_runtime.last_signature_s = request.enqueued_s
                        if (
                            send_initial_data
                            and not initial_data_phase_complete
                            and prioritize_forward_data
                        ):
                            time.sleep(max(0.0, self.config.repair_worker_poll_interval_s))

                parallel_threads.extend(
                    [
                        threading.Thread(target=_recv_pump, name="ssync-recv-pump", daemon=True),
                        threading.Thread(
                            target=_repair_worker,
                            name="ssync-repair-worker",
                            daemon=True,
                        ),
                    ]
                )
                for thread in parallel_threads:
                    thread.start()

            def _stop_parallel_repairs() -> None:
                nonlocal parallel_runtime, completed, repair_rounds, repaired_chunks
                nonlocal last_uplink_activity_s
                if parallel_runtime is None:
                    return
                parallel_runtime.stop_event.set()
                for thread in parallel_threads:
                    thread.join(timeout=1.0)
                with parallel_runtime.state_lock:
                    repaired_chunks += parallel_runtime.repaired_chunks
                    # Keep total observability, but do not charge worker rounds
                    # against feedback-budget termination logic.
                    repair_rounds += parallel_runtime.repair_rounds
                    completed = parallel_runtime.completed
                    if parallel_runtime.hash_mismatch:
                        completed = False
                    if parallel_runtime.saw_uplink:
                        last_uplink_activity_s = time.monotonic()
                parallel_threads.clear()
                parallel_runtime = None

            if auto_feedback_enabled and not feedback_active and _probe_uplink_packets():
                _set_feedback_mode(True, "auto_uplink_packet_detected")
                if send_initial_data and feedback_active:
                    _start_parallel_repairs()

            last_beacon_s = 0.0
            last_metadata_s = time.monotonic()
            chunks_since_metadata = 0
            for _ in range(self.config.manifest_repeats):
                if self._should_stop(stop_requested):
                    return self._aborted_result(manifest.transfer_id.hex(), total_chunks)
                if not self._sendto_with_interrupt(
                    sock=sock,
                    payload=encode_metadata(manifest),
                    destination=destination,
                    stop_requested=stop_requested,
                ):
                    return self._aborted_result(manifest.transfer_id.hex(), total_chunks)
                last_beacon_s = self._maybe_send_beacon(
                    sock=sock,
                    destination=destination,
                    transfer_id=manifest.transfer_id,
                    last_beacon_s=last_beacon_s,
                )
                if self.config.inter_packet_delay_s > 0:
                    time.sleep(self.config.inter_packet_delay_s)
                if auto_feedback_enabled and not feedback_active and _probe_uplink_packets():
                    _set_feedback_mode(True, "auto_uplink_packet_detected")
            last_metadata_s = time.monotonic()
            chunks_since_metadata = 0
            if total_chunks == 0:
                LOGGER.debug(
                    "transfer_id=%s zero_chunk_transfer_fast_path_after_metadata",
                    transfer_id_hex,
                )
                return SendResult(
                    transfer_id_hex=manifest.transfer_id.hex(),
                    total_chunks=0,
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=True,
                )

            dropped = 0
            completed_by_receiver_signal = False
            if send_initial_data:
                if feedback_active:
                    _start_parallel_repairs()
                try:
                    next_probe_s = time.monotonic()
                    for chunk_index in range(total_chunks):
                        if (
                            auto_feedback_enabled
                            and feedback_active
                            and auto_feedback_timeout_s > 0
                            and last_uplink_activity_s is not None
                            and (time.monotonic() - last_uplink_activity_s)
                            >= auto_feedback_timeout_s
                        ):
                            _set_feedback_mode(False, "auto_uplink_idle_timeout")
                        if self._should_stop(stop_requested):
                            return self._aborted_result(manifest.transfer_id.hex(), total_chunks)
                        chunk_payload = chunk_reader(chunk_index)
                        if not chunk_payload:
                            continue
                        last_beacon_s = self._maybe_send_beacon(
                            sock=sock,
                            destination=destination,
                            transfer_id=manifest.transfer_id,
                            last_beacon_s=last_beacon_s,
                        )
                        if feedback_active and not prioritize_forward_data:
                            if parallel_runtime is None:
                                (
                                    repaired_now,
                                    rounds_now,
                                    paced_data_bytes,
                                    completed_now,
                                    saw_uplink_now,
                                ) = self._drain_repair_requests(
                                    sock=sock,
                                    manifest=manifest,
                                    total_chunks=total_chunks,
                                    chunk_reader=chunk_reader,
                                    destination=destination,
                                    paced_start_s=paced_start_s,
                                    paced_data_bytes=paced_data_bytes,
                                    max_rounds=self.config.midstream_repair_max_rounds_per_poll,
                                    max_chunks=self.config.midstream_repair_max_chunks_per_poll,
                                )
                                if saw_uplink_now:
                                    last_uplink_activity_s = time.monotonic()
                                repaired_chunks += repaired_now
                                repair_rounds += rounds_now
                                feedback_budget_rounds += rounds_now
                                if completed_now:
                                    completed = True
                                    completed_by_receiver_signal = True
                                    LOGGER.debug(
                                        "transfer_id=%s midstream_transfer_complete_short_circuit",
                                        transfer_id_hex,
                                    )
                                    break
                                if rounds_now:
                                    LOGGER.debug(
                                        (
                                            "transfer_id=%s serviced_midstream_repairs "
                                            "rounds=%d chunks=%d"
                                        ),
                                        transfer_id_hex,
                                        rounds_now,
                                        repaired_now,
                                    )
                                if _feedback_budget_exhausted():
                                    LOGGER.debug(
                                        (
                                            "transfer_id=%s "
                                            "reached_feedback_budget_after_midstream_repairs %s"
                                        ),
                                        transfer_id_hex,
                                        budget_stop_reason,
                                    )
                                    break
                        elif (
                            auto_feedback_enabled
                            and not feedback_active
                            and (
                                (
                                    self.config.auto_feedback_probe_interval_chunks > 0
                                    and (chunk_index + 1)
                                    % self.config.auto_feedback_probe_interval_chunks
                                    == 0
                                )
                                or time.monotonic() >= next_probe_s
                            )
                        ):
                            next_probe_s = time.monotonic() + 0.25
                            if _probe_uplink_packets():
                                _set_feedback_mode(True, "auto_uplink_packet_detected")
                                if send_initial_data and feedback_active:
                                    _start_parallel_repairs()
                        should_drop = (
                            self.config.drop_every_nth_data > 0
                            and (chunk_index + 1) % self.config.drop_every_nth_data == 0
                        )
                        if should_drop:
                            dropped += 1
                            continue
                        if not self._sendto_with_interrupt(
                            sock=sock,
                            payload=encode_data_chunk(
                                manifest.transfer_id, chunk_index, chunk_payload
                            ),
                            destination=destination,
                            stop_requested=stop_requested,
                        ):
                            return self._aborted_result(manifest.transfer_id.hex(), total_chunks)
                        paced_data_bytes = self._apply_rate_limit(
                            paced_start_s=paced_start_s,
                            paced_data_bytes=paced_data_bytes,
                            just_sent_bytes=len(chunk_payload),
                        )
                        chunks_since_metadata += 1
                        (
                            last_metadata_s,
                            chunks_since_metadata,
                        ) = self._maybe_send_periodic_metadata(
                            sock=sock,
                            destination=destination,
                            manifest=manifest,
                            last_metadata_s=last_metadata_s,
                            chunks_since_metadata=chunks_since_metadata,
                        )
                        if self.config.inter_packet_delay_s > 0:
                            time.sleep(self.config.inter_packet_delay_s)
                        if chunk_index > 0 and chunk_index % 4096 == 0:
                            LOGGER.debug(
                                "transfer_id=%s sent_chunk_progress=%d/%d",
                                transfer_id_hex,
                                chunk_index + 1,
                                total_chunks,
                            )
                finally:
                    initial_data_phase_complete = True
                    _stop_parallel_repairs()
                    if completed:
                        completed_by_receiver_signal = True

            if (
                feedback_active
                and not completed_by_receiver_signal
                and not _feedback_budget_exhausted()
            ):
                last_beacon_s = self._maybe_send_beacon(
                    sock=sock,
                    destination=destination,
                    transfer_id=manifest.transfer_id,
                    last_beacon_s=last_beacon_s,
                )
                (
                    repaired_now,
                    rounds_now,
                    paced_data_bytes,
                    completed_now,
                    saw_uplink_now,
                ) = self._drain_repair_requests(
                    sock=sock,
                    manifest=manifest,
                    total_chunks=total_chunks,
                    chunk_reader=chunk_reader,
                    destination=destination,
                    paced_start_s=paced_start_s,
                    paced_data_bytes=paced_data_bytes,
                    max_rounds=self.config.midstream_repair_max_rounds_per_poll,
                    max_chunks=self.config.midstream_repair_max_chunks_per_poll,
                )
                if saw_uplink_now:
                    last_uplink_activity_s = time.monotonic()
                repaired_chunks += repaired_now
                repair_rounds += rounds_now
                feedback_budget_rounds += rounds_now
                if completed_now:
                    completed = True
                    completed_by_receiver_signal = True
                if rounds_now:
                    LOGGER.debug(
                        "transfer_id=%s serviced_pre_fin_repairs rounds=%d chunks=%d",
                        transfer_id_hex,
                        rounds_now,
                        repaired_now,
                    )
            elif feedback_active and _feedback_budget_exhausted():
                LOGGER.debug(
                    "transfer_id=%s skipping_pre_fin_feedback_due_to_budget %s",
                    transfer_id_hex,
                    budget_stop_reason,
                )
            if completed_by_receiver_signal:
                LOGGER.debug(
                    "transfer_id=%s transfer_completed_before_feedback_wait",
                    transfer_id_hex,
                )

            if (
                auto_feedback_enabled
                and not feedback_active
                and _probe_uplink_packets()
            ):
                _set_feedback_mode(True, "auto_uplink_packet_detected")

            if not feedback_active:
                if auto_feedback_enabled:
                    self._auto_feedback_active = False
                    self._last_auto_uplink_activity_s = last_uplink_activity_s
                LOGGER.info(
                    "send done transfer_id=%s completed=%s dropped_initial=%d",
                    transfer_id_hex,
                    dropped == 0,
                    dropped,
                )
                return SendResult(
                    transfer_id_hex=manifest.transfer_id.hex(),
                    total_chunks=total_chunks,
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=(dropped == 0),
                )

            post_fin_repair_rounds = 0
            if not completed_by_receiver_signal:
                completed = False
            idle_timeouts = 0
            suppressed_duplicate_repairs = 0
            last_post_fin_signature: tuple[tuple[int, int], ...] | None = None
            last_post_fin_service_s = 0.0
            last_post_fin_activity_s = time.monotonic()
            if completed_by_receiver_signal:
                return SendResult(
                    transfer_id_hex=manifest.transfer_id.hex(),
                    total_chunks=total_chunks,
                    repaired_chunks=repaired_chunks,
                    repair_rounds=repair_rounds,
                    completed=True,
                )
            sock.setblocking(True)
            sock.settimeout(self.config.feedback_wait_s)
            while effective_max_repair_rounds <= 0 or (
                post_fin_repair_rounds < effective_max_repair_rounds
            ):
                if (
                    auto_feedback_enabled
                    and feedback_active
                    and auto_feedback_timeout_s > 0
                    and last_uplink_activity_s is not None
                    and (time.monotonic() - last_uplink_activity_s) >= auto_feedback_timeout_s
                ):
                    _set_feedback_mode(False, "auto_uplink_idle_timeout")
                    break
                if _feedback_budget_exhausted():
                    LOGGER.warning(
                        "transfer_id=%s reached_feedback_budget %s",
                        transfer_id_hex,
                        budget_stop_reason,
                    )
                    break
                if self._should_stop(stop_requested):
                    return self._aborted_result(manifest.transfer_id.hex(), total_chunks)
                now = time.monotonic()
                if now - last_post_fin_activity_s >= self.config.feedback_wait_s:
                    idle_timeouts += 1
                    LOGGER.debug(
                        "transfer_id=%s post_fin_timeout idle=%d/%d",
                        transfer_id_hex,
                        idle_timeouts,
                        self.config.max_feedback_idle_timeouts,
                    )
                    # Under lossy links, metadata can be dropped. Re-send METADATA so
                    # receiver can (re)associate transfer state and request missing data.
                    if self._sendto_with_interrupt(
                        sock=sock,
                        payload=encode_metadata(manifest),
                        destination=destination,
                        stop_requested=stop_requested,
                    ):
                        LOGGER.debug(
                            "transfer_id=%s resent_metadata_after_feedback_timeout",
                            transfer_id_hex,
                        )
                    else:
                        return self._aborted_result(manifest.transfer_id.hex(), total_chunks)
                    last_post_fin_activity_s = now
                    if idle_timeouts >= self.config.max_feedback_idle_timeouts:
                        LOGGER.warning(
                            "transfer_id=%s stopping_after_idle_timeouts",
                            transfer_id_hex,
                        )
                        break
                last_beacon_s = self._maybe_send_beacon(
                    sock=sock,
                    destination=destination,
                    transfer_id=manifest.transfer_id,
                    last_beacon_s=last_beacon_s,
                )
                try:
                    response_raw, _response_addr = sock.recvfrom(65535)
                except (BlockingIOError, TimeoutError):
                    continue
                try:
                    parsed = decode_frame(response_raw)
                except ValueError:
                    continue
                if parsed.frame_type is None:
                    continue
                last_uplink_activity_s = time.monotonic()
                if parsed.frame_type == FrameType.STATUS:
                    status = decode_status(parsed.payload)
                    if status.kind != StatusKind.TRANSFER:
                        continue
                    if status.transfer_id != manifest.transfer_id:
                        continue
                    idle_timeouts = 0
                    last_post_fin_activity_s = time.monotonic()
                    LOGGER.debug(
                        "transfer_id=%s status=%s missing=%s",
                        transfer_id_hex,
                        status.state.name,
                        summarize_ranges(status.missing_ranges),
                    )
                    if status.state == TransferState.COMPLETE:
                        completed = True
                        break
                    if status.state == TransferState.HASH_MISMATCH:
                        completed = False
                        break
                    if status.state != TransferState.INCOMPLETE:
                        continue
                    if not status.missing_ranges:
                        continue
                    request_signature = tuple(status.missing_ranges)
                    now = time.monotonic()
                    if (
                        self.config.repair_duplicate_suppression_s > 0
                        and last_post_fin_signature == request_signature
                        and now - last_post_fin_service_s
                        < self.config.repair_duplicate_suppression_s
                    ):
                        suppressed_duplicate_repairs += 1
                        continue
                    repaired_now, _, paced_data_bytes = self._send_requested_repairs(
                        sock=sock,
                        manifest=manifest,
                        total_chunks=total_chunks,
                        chunk_reader=chunk_reader,
                        destination=destination,
                        missing_ranges=status.missing_ranges,
                        paced_start_s=paced_start_s,
                        paced_data_bytes=paced_data_bytes,
                    )
                    repaired_chunks += repaired_now
                    repair_rounds += 1
                    feedback_budget_rounds += 1
                    post_fin_repair_rounds += 1
                    last_post_fin_signature = request_signature
                    last_post_fin_service_s = now
                    continue
            if (
                effective_max_repair_rounds > 0
                and post_fin_repair_rounds >= effective_max_repair_rounds
                and not completed
            ):
                LOGGER.warning(
                    "transfer_id=%s reached_max_repair_rounds=%d without completion",
                    transfer_id_hex,
                    effective_max_repair_rounds,
                )

        LOGGER.info(
            (
                "send done transfer_id=%s completed=%s repaired_chunks=%d "
                "repair_rounds=%d suppressed_duplicates=%d"
            ),
            transfer_id_hex,
            completed,
            repaired_chunks,
            repair_rounds,
            suppressed_duplicate_repairs if feedback_active else 0,
        )
        if auto_feedback_enabled:
            self._auto_feedback_active = feedback_active
            self._last_auto_uplink_activity_s = last_uplink_activity_s
        return SendResult(
            transfer_id_hex=manifest.transfer_id.hex(),
            total_chunks=total_chunks,
            repaired_chunks=repaired_chunks,
            repair_rounds=repair_rounds,
            completed=completed,
        )

    @staticmethod
    def _should_stop(stop_requested: Callable[[], bool] | None) -> bool:
        return bool(stop_requested is not None and stop_requested())

    def _aborted_result(self, transfer_id_hex: str, total_chunks: int) -> SendResult:
        LOGGER.warning("send aborted transfer_id=%s", transfer_id_hex)
        return SendResult(
            transfer_id_hex=transfer_id_hex,
            total_chunks=total_chunks,
            repaired_chunks=0,
            repair_rounds=0,
            completed=False,
        )

    def _sendto_with_interrupt(
        self,
        *,
        sock: socket.socket,
        payload: bytes,
        destination: tuple[str, int],
        stop_requested: Callable[[], bool] | None,
    ) -> bool:
        while True:
            try:
                sock.sendto(payload, destination)
                return True
            except BlockingIOError:
                if self._should_stop(stop_requested):
                    return False
                time.sleep(0.001)
            except TimeoutError:
                if self._should_stop(stop_requested):
                    return False
                if self.config.enable_feedback:
                    raise
                if stop_requested is None:
                    # Avoid spinning forever for direct callers that do not provide
                    # cooperative cancellation in open-loop mode.
                    raise

    def _maybe_send_beacon(
        self,
        *,
        sock: socket.socket,
        destination: tuple[str, int],
        transfer_id: bytes,
        last_beacon_s: float,
    ) -> float:
        if self.config.beacon_interval_s <= 0:
            return last_beacon_s
        now = time.monotonic()
        if last_beacon_s > 0 and now - last_beacon_s < self.config.beacon_interval_s:
            return last_beacon_s
        try:
            sock.sendto(encode_beacon(BeaconRole.SENDER, transfer_id), destination)
        except BlockingIOError:
            return last_beacon_s
        return now

    def _maybe_send_periodic_metadata(
        self,
        *,
        sock: socket.socket,
        destination: tuple[str, int],
        manifest: TransferManifest,
        last_metadata_s: float,
        chunks_since_metadata: int,
    ) -> tuple[float, int]:
        interval_due = (
            self.config.periodic_metadata_interval_s > 0
            and (time.monotonic() - last_metadata_s) >= self.config.periodic_metadata_interval_s
        )
        chunk_due = (
            self.config.periodic_metadata_every_n_chunks > 0
            and chunks_since_metadata >= self.config.periodic_metadata_every_n_chunks
        )
        if not interval_due and not chunk_due:
            return last_metadata_s, chunks_since_metadata
        try:
            sock.sendto(encode_metadata(manifest), destination)
        except BlockingIOError:
            return last_metadata_s, chunks_since_metadata
        return time.monotonic(), 0

    def query_remote_file(
        self,
        *,
        destination_host: str,
        destination_port: int,
        remote_name: str,
        include_checksum: bool,
    ) -> RemoteFileInfo:
        destination = (destination_host, destination_port)
        query_token = str(time.time_ns()).encode("utf-8")
        query_metadata = {
            int(MetadataType.FILE_INFO_QUERY_PATH): remote_name.encode("utf-8"),
            int(MetadataType.FILE_INFO_QUERY_INCLUDE_CHECKSUM): (
                b"\x01" if include_checksum else b"\x00"
            ),
            int(MetadataType.FILE_INFO_QUERY_TOKEN): query_token,
        }
        query_manifest = TransferManifest.from_bytes(
            raw=b"",
            file_name="__status_query__",
            chunk_size=1,
            metadata=query_metadata,
        )
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.config.feedback_wait_s)
            sock.sendto(encode_metadata(query_manifest), destination)
            deadline = time.monotonic() + self.config.feedback_wait_s
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for matching STATUS FILE_INFO_RESPONSE")
                sock.settimeout(remaining)
                response_raw, _ = sock.recvfrom(65535)
                parsed = decode_frame(response_raw)
                if parsed.frame_type != FrameType.STATUS:
                    continue
                status = decode_status(parsed.payload)
                if status.kind != StatusKind.FILE_INFO_RESPONSE:
                    continue
                if status.query_token != query_token:
                    continue
                if status.file_info is None:
                    continue
                if status.file_info.path != remote_name:
                    continue
                return status.file_info

    @staticmethod
    def local_file_checksum(file_path: Path) -> bytes:
        digest = sha256()
        with file_path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.digest()

    @staticmethod
    def _read_chunk(*, file_stream: BinaryIO, chunk_size: int, chunk_index: int) -> bytes:
        offset = chunk_index * chunk_size
        file_stream.seek(offset)
        return file_stream.read(chunk_size)

    def _send_requested_repairs(
        self,
        *,
        sock: socket.socket,
        manifest: TransferManifest,
        total_chunks: int,
        chunk_reader: Callable[[int], bytes],
        destination: tuple[str, int],
        missing_ranges: list[tuple[int, int]],
        paced_start_s: float,
        paced_data_bytes: int,
    ) -> tuple[int, bool, int]:
        indexes = expand_ranges(missing_ranges)
        if not indexes:
            return 0, True, paced_data_bytes
        repaired = 0
        for chunk_index in indexes:
            if chunk_index >= total_chunks:
                continue
            chunk_payload = chunk_reader(chunk_index)
            if not chunk_payload:
                continue
            while True:
                try:
                    sock.sendto(
                        encode_data_chunk(manifest.transfer_id, chunk_index, chunk_payload),
                        destination,
                    )
                    break
                except BlockingIOError:
                    time.sleep(0.001)
            paced_data_bytes = self._apply_rate_limit(
                paced_start_s=paced_start_s,
                paced_data_bytes=paced_data_bytes,
                just_sent_bytes=len(chunk_payload),
            )
            repaired += 1
            if self.config.inter_packet_delay_s > 0:
                time.sleep(self.config.inter_packet_delay_s)
        return repaired, False, paced_data_bytes

    def _drain_repair_requests(
        self,
        *,
        sock: socket.socket,
        manifest: TransferManifest,
        total_chunks: int,
        chunk_reader: Callable[[int], bytes],
        destination: tuple[str, int],
        paced_start_s: float,
        paced_data_bytes: int,
        max_rounds: int,
        max_chunks: int,
    ) -> tuple[int, int, int, bool, bool]:
        repaired_chunks = 0
        repair_rounds = 0
        completed = False
        saw_uplink = False
        while True:
            if max_rounds > 0 and repair_rounds >= max_rounds:
                break
            if max_chunks > 0 and repaired_chunks >= max_chunks:
                break
            try:
                response_raw, _response_addr = sock.recvfrom(65535)
            except (BlockingIOError, TimeoutError):
                break
            saw_uplink = True
            try:
                parsed = decode_frame(response_raw)
            except ValueError:
                continue
            if parsed.frame_type == FrameType.STATUS:
                status = decode_status(parsed.payload)
                if status.kind != StatusKind.TRANSFER:
                    continue
                if status.transfer_id != manifest.transfer_id:
                    continue
                if status.state == TransferState.COMPLETE:
                    completed = True
                    break
                if status.state != TransferState.INCOMPLETE or not status.missing_ranges:
                    continue
                remaining_chunks = 0 if max_chunks <= 0 else max_chunks - repaired_chunks
                if max_chunks > 0 and remaining_chunks <= 0:
                    break
                repair_ranges = (
                    status.missing_ranges
                    if max_chunks <= 0
                    else limit_ranges_to_chunk_budget(
                        status.missing_ranges,
                        remaining_chunks,
                    )
                )
                repaired_now, _, paced_data_bytes = self._send_requested_repairs(
                    sock=sock,
                    manifest=manifest,
                    total_chunks=total_chunks,
                    chunk_reader=chunk_reader,
                    destination=destination,
                    missing_ranges=repair_ranges,
                    paced_start_s=paced_start_s,
                    paced_data_bytes=paced_data_bytes,
                )
                repaired_chunks += repaired_now
                repair_rounds += 1
                continue
        return repaired_chunks, repair_rounds, paced_data_bytes, completed, saw_uplink

    def _apply_rate_limit(
        self,
        *,
        paced_start_s: float,
        paced_data_bytes: int,
        just_sent_bytes: int,
    ) -> int:
        new_total = paced_data_bytes + just_sent_bytes
        if self.config.max_data_rate_bps <= 0:
            return new_total
        elapsed_s = time.monotonic() - paced_start_s
        # Convert payload bytes to bits so the configured limit matches the CLI name.
        target_elapsed_s = (new_total * 8) / float(self.config.max_data_rate_bps)
        if target_elapsed_s > elapsed_s:
            time.sleep(target_elapsed_s - elapsed_s)
        return new_total

