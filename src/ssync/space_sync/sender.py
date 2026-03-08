from __future__ import annotations

import logging
import math
import socket
import time
from hashlib import sha256
from pathlib import Path

from .frames import (
    decode_file_info_response,
    decode_frame,
    decode_repair_request,
    decode_status,
    decode_transfer_complete,
    encode_data_chunk,
    encode_file_info_request,
    encode_fin,
    encode_manifest,
    encode_repair_done,
)
from .manifest import TransferManifest
from .ranges import expand_ranges, summarize_ranges
from .types import FrameType, MetadataType, RemoteFileInfo, SenderConfig, SendResult, TransferState

LOGGER = logging.getLogger(__name__)
class SpaceSyncSender:
    def __init__(self, config: SenderConfig | None = None) -> None:
        self.config = config or SenderConfig()

    def _chunks(self, payload: bytes, chunk_size: int) -> list[bytes]:
        if not payload:
            return []
        return [
            payload[offset : offset + chunk_size]
            for offset in range(0, len(payload), chunk_size)
        ]

    def send_file(
        self,
        file_path: Path,
        destination_host: str,
        destination_port: int,
        remote_name: str | None = None,
    ) -> SendResult:
        file_path = file_path.resolve()
        raw = file_path.read_bytes()
        file_stat = file_path.stat()
        metadata = {
            int(MetadataType.SOURCE_MTIME_NS): int(file_stat.st_mtime_ns).to_bytes(8, "big"),
        }
        manifest = TransferManifest.from_bytes(
            raw=raw,
            file_name=remote_name or file_path.name,
            chunk_size=self.config.chunk_size,
            metadata=metadata,
        )
        chunks = self._chunks(raw, self.config.chunk_size)
        destination = (destination_host, destination_port)
        repaired_chunks = 0
        repair_rounds = 0
        completed = True
        paced_start_s = time.monotonic()
        paced_data_bytes = 0
        transfer_id_hex = manifest.transfer_id.hex()
        LOGGER.info(
            "send start transfer_id=%s file=%s remote=%s chunks=%d feedback=%s",
            transfer_id_hex,
            file_path,
            manifest.file_name,
            len(chunks),
            self.config.enable_feedback,
        )
        if (
            self.config.enable_feedback
            and self.config.inter_packet_delay_s <= 0
            and len(chunks) > 32768
        ):
            LOGGER.warning(
                "transfer_id=%s zero inter-packet delay on large transfer may cause heavy loss",
                transfer_id_hex,
            )

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.config.feedback_wait_s)
            for _ in range(self.config.manifest_repeats):
                sock.sendto(encode_manifest(manifest), destination)
                if self.config.inter_packet_delay_s > 0:
                    time.sleep(self.config.inter_packet_delay_s)

            dropped = 0
            if self.config.enable_feedback:
                sock.setblocking(False)
            for chunk_index, chunk_payload in enumerate(chunks):
                if self.config.enable_feedback:
                    repaired_now, rounds_now, paced_data_bytes = self._drain_repair_requests(
                        sock=sock,
                        manifest=manifest,
                        chunks=chunks,
                        destination=destination,
                        send_repair_done=False,
                        paced_start_s=paced_start_s,
                        paced_data_bytes=paced_data_bytes,
                        max_rounds=self.config.midstream_repair_max_rounds_per_poll,
                        max_chunks=self.config.midstream_repair_max_chunks_per_poll,
                    )
                    repaired_chunks += repaired_now
                    repair_rounds += rounds_now
                    if rounds_now:
                        LOGGER.debug(
                            "transfer_id=%s serviced_midstream_repairs rounds=%d chunks=%d",
                            transfer_id_hex,
                            rounds_now,
                            repaired_now,
                        )
                should_drop = (
                    self.config.drop_every_nth_data > 0
                    and (chunk_index + 1) % self.config.drop_every_nth_data == 0
                )
                if should_drop:
                    dropped += 1
                    continue
                sock.sendto(
                    encode_data_chunk(manifest.transfer_id, chunk_index, chunk_payload),
                    destination,
                )
                paced_data_bytes = self._apply_rate_limit(
                    paced_start_s=paced_start_s,
                    paced_data_bytes=paced_data_bytes,
                    just_sent_bytes=len(chunk_payload),
                )
                if self.config.inter_packet_delay_s > 0:
                    time.sleep(self.config.inter_packet_delay_s)
                if chunk_index > 0 and chunk_index % 4096 == 0:
                    LOGGER.debug(
                        "transfer_id=%s sent_chunk_progress=%d/%d",
                        transfer_id_hex,
                        chunk_index + 1,
                        len(chunks),
                    )

            if self.config.enable_feedback:
                repaired_now, rounds_now, paced_data_bytes = self._drain_repair_requests(
                    sock=sock,
                    manifest=manifest,
                    chunks=chunks,
                    destination=destination,
                    send_repair_done=False,
                    paced_start_s=paced_start_s,
                    paced_data_bytes=paced_data_bytes,
                    max_rounds=self.config.midstream_repair_max_rounds_per_poll,
                    max_chunks=self.config.midstream_repair_max_chunks_per_poll,
                )
                repaired_chunks += repaired_now
                repair_rounds += rounds_now
                if rounds_now:
                    LOGGER.debug(
                        "transfer_id=%s serviced_pre_fin_repairs rounds=%d chunks=%d",
                        transfer_id_hex,
                        rounds_now,
                        repaired_now,
                    )
            sock.sendto(encode_fin(manifest.transfer_id), destination)
            LOGGER.debug("transfer_id=%s sent_fin", transfer_id_hex)

            if not self.config.enable_feedback:
                LOGGER.info(
                    "send done transfer_id=%s completed=%s dropped_initial=%d",
                    transfer_id_hex,
                    dropped == 0,
                    dropped,
                )
                return SendResult(
                    transfer_id_hex=manifest.transfer_id.hex(),
                    total_chunks=len(chunks),
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=(dropped == 0),
                )

            post_fin_repair_rounds = 0
            completed = False
            idle_timeouts = 0
            suppressed_duplicate_repairs = 0
            last_post_fin_signature: tuple[tuple[int, int], ...] | None = None
            last_post_fin_service_s = 0.0
            sock.setblocking(True)
            sock.settimeout(self.config.feedback_wait_s)
            while self.config.max_repair_rounds <= 0 or (
                post_fin_repair_rounds < self.config.max_repair_rounds
            ):
                try:
                    response_raw, response_addr = sock.recvfrom(65535)
                except TimeoutError:
                    idle_timeouts += 1
                    LOGGER.debug(
                        "transfer_id=%s post_fin_timeout idle=%d/%d",
                        transfer_id_hex,
                        idle_timeouts,
                        self.config.max_feedback_idle_timeouts,
                    )
                    if idle_timeouts >= self.config.max_feedback_idle_timeouts:
                        LOGGER.warning(
                            "transfer_id=%s stopping_after_idle_timeouts",
                            transfer_id_hex,
                        )
                        break
                    continue
                try:
                    parsed = decode_frame(response_raw)
                except ValueError:
                    continue
                idle_timeouts = 0
                if parsed.frame_type is None:
                    continue
                if parsed.frame_type == FrameType.STATUS:
                    status = decode_status(parsed.payload)
                    if status.transfer_id != manifest.transfer_id:
                        continue
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
                    # Treat an incomplete STATUS as a hint but require explicit repair request.
                    continue
                if parsed.frame_type == FrameType.TRANSFER_COMPLETE:
                    complete_transfer_id = decode_transfer_complete(parsed.payload)
                    if complete_transfer_id != manifest.transfer_id:
                        continue
                    LOGGER.debug("transfer_id=%s received_transfer_complete", transfer_id_hex)
                    completed = True
                    break
                if parsed.frame_type != FrameType.REPAIR_REQUEST:
                    continue
                request = decode_repair_request(parsed.payload)
                if request.transfer_id != manifest.transfer_id:
                    continue
                request_signature = tuple(request.missing_ranges)
                now = time.monotonic()
                if (
                    self.config.repair_duplicate_suppression_s > 0
                    and last_post_fin_signature == request_signature
                    and now - last_post_fin_service_s
                    < self.config.repair_duplicate_suppression_s
                ):
                    suppressed_duplicate_repairs += 1
                    if suppressed_duplicate_repairs % 50 == 1:
                        LOGGER.debug(
                            "transfer_id=%s suppressed_duplicate_repair_requests=%d",
                            transfer_id_hex,
                            suppressed_duplicate_repairs,
                        )
                    sock.sendto(encode_repair_done(manifest.transfer_id), response_addr)
                    continue
                LOGGER.debug(
                    "transfer_id=%s post_fin_repair_request missing=%s",
                    transfer_id_hex,
                    summarize_ranges(request.missing_ranges),
                )
                repaired_now, _, paced_data_bytes = self._send_requested_repairs(
                    sock=sock,
                    manifest=manifest,
                    chunks=chunks,
                    destination=destination,
                    missing_ranges=request.missing_ranges,
                    paced_start_s=paced_start_s,
                    paced_data_bytes=paced_data_bytes,
                )
                repaired_chunks += repaired_now
                sock.sendto(encode_repair_done(manifest.transfer_id), response_addr)
                repair_rounds += 1
                post_fin_repair_rounds += 1
                last_post_fin_signature = request_signature
                last_post_fin_service_s = now
                LOGGER.debug(
                    "transfer_id=%s post_fin_repair_done round=%d repaired_now=%d",
                    transfer_id_hex,
                    post_fin_repair_rounds,
                    repaired_now,
                )
            if (
                self.config.max_repair_rounds > 0
                and post_fin_repair_rounds >= self.config.max_repair_rounds
                and not completed
            ):
                LOGGER.warning(
                    "transfer_id=%s reached_max_repair_rounds=%d without completion",
                    transfer_id_hex,
                    self.config.max_repair_rounds,
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
            suppressed_duplicate_repairs if self.config.enable_feedback else 0,
        )
        return SendResult(
            transfer_id_hex=manifest.transfer_id.hex(),
            total_chunks=math.ceil(len(raw) / self.config.chunk_size) if raw else 0,
            repaired_chunks=repaired_chunks,
            repair_rounds=repair_rounds,
            completed=completed,
        )

    def query_remote_file(
        self,
        *,
        destination_host: str,
        destination_port: int,
        remote_name: str,
        include_checksum: bool,
    ) -> RemoteFileInfo:
        destination = (destination_host, destination_port)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.config.feedback_wait_s)
            sock.sendto(encode_file_info_request(remote_name, include_checksum), destination)
            deadline = time.monotonic() + self.config.feedback_wait_s
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for matching FILE_INFO_RESPONSE")
                sock.settimeout(remaining)
                response_raw, _ = sock.recvfrom(65535)
                parsed = decode_frame(response_raw)
                if parsed.frame_type != FrameType.FILE_INFO_RESPONSE:
                    continue
                response = decode_file_info_response(parsed.payload)
                if response.path != remote_name:
                    continue
                return response

    @staticmethod
    def local_file_checksum(file_path: Path) -> bytes:
        return sha256(file_path.read_bytes()).digest()

    def _send_requested_repairs(
        self,
        *,
        sock: socket.socket,
        manifest: TransferManifest,
        chunks: list[bytes],
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
            if chunk_index >= len(chunks):
                continue
            sock.sendto(
                encode_data_chunk(manifest.transfer_id, chunk_index, chunks[chunk_index]),
                destination,
            )
            paced_data_bytes = self._apply_rate_limit(
                paced_start_s=paced_start_s,
                paced_data_bytes=paced_data_bytes,
                just_sent_bytes=len(chunks[chunk_index]),
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
        chunks: list[bytes],
        destination: tuple[str, int],
        send_repair_done: bool,
        paced_start_s: float,
        paced_data_bytes: int,
        max_rounds: int,
        max_chunks: int,
    ) -> tuple[int, int, int]:
        repaired_chunks = 0
        repair_rounds = 0
        while True:
            if max_rounds > 0 and repair_rounds >= max_rounds:
                break
            if max_chunks > 0 and repaired_chunks >= max_chunks:
                break
            try:
                response_raw, response_addr = sock.recvfrom(65535)
            except (BlockingIOError, TimeoutError):
                break
            try:
                parsed = decode_frame(response_raw)
            except ValueError:
                continue
            if parsed.frame_type != FrameType.REPAIR_REQUEST:
                continue
            request = decode_repair_request(parsed.payload)
            if request.transfer_id != manifest.transfer_id:
                continue
            LOGGER.debug(
                "transfer_id=%s midstream_repair_request missing=%s",
                manifest.transfer_id.hex(),
                summarize_ranges(request.missing_ranges),
            )
            repaired_now, _, paced_data_bytes = self._send_requested_repairs(
                sock=sock,
                manifest=manifest,
                chunks=chunks,
                destination=destination,
                missing_ranges=request.missing_ranges,
                paced_start_s=paced_start_s,
                paced_data_bytes=paced_data_bytes,
            )
            repaired_chunks += repaired_now
            if send_repair_done:
                sock.sendto(encode_repair_done(manifest.transfer_id), response_addr)
            repair_rounds += 1
        return repaired_chunks, repair_rounds, paced_data_bytes

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

