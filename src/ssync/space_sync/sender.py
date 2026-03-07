from __future__ import annotations

import math
import socket
import time
from pathlib import Path

from .frames import (
    decode_frame,
    decode_repair_request,
    decode_status,
    encode_data_chunk,
    encode_fin,
    encode_manifest,
    encode_repair_done,
)
from .manifest import TransferManifest
from .ranges import expand_ranges
from .types import FrameType, SendResult, SenderConfig, TransferState


class SpaceSyncSender:
    def __init__(self, config: SenderConfig | None = None) -> None:
        self.config = config or SenderConfig()

    def _chunks(self, payload: bytes, chunk_size: int) -> list[bytes]:
        if not payload:
            return []
        return [payload[offset : offset + chunk_size] for offset in range(0, len(payload), chunk_size)]

    def send_file(
        self,
        file_path: Path,
        destination_host: str,
        destination_port: int,
        remote_name: str | None = None,
    ) -> SendResult:
        file_path = file_path.resolve()
        raw = file_path.read_bytes()
        manifest = TransferManifest.from_file(
            file_path=file_path,
            chunk_size=self.config.chunk_size,
            remote_name=remote_name,
        )
        chunks = self._chunks(raw, self.config.chunk_size)
        destination = (destination_host, destination_port)
        repaired_chunks = 0
        repair_rounds = 0
        completed = True

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.config.feedback_wait_s)
            for _ in range(self.config.manifest_repeats):
                sock.sendto(encode_manifest(manifest), destination)
                if self.config.inter_packet_delay_s > 0:
                    time.sleep(self.config.inter_packet_delay_s)

            dropped = 0
            for chunk_index, chunk_payload in enumerate(chunks):
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
                if self.config.inter_packet_delay_s > 0:
                    time.sleep(self.config.inter_packet_delay_s)

            sock.sendto(encode_fin(manifest.transfer_id), destination)

            if not self.config.enable_feedback:
                return SendResult(
                    transfer_id_hex=manifest.transfer_id.hex(),
                    total_chunks=len(chunks),
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=(dropped == 0),
                )

            repair_rounds = 0
            while repair_rounds < self.config.max_repair_rounds:
                try:
                    response_raw, response_addr = sock.recvfrom(65535)
                except TimeoutError:
                    break
                try:
                    parsed = decode_frame(response_raw)
                except ValueError:
                    continue
                if parsed.frame_type == FrameType.STATUS:
                    status = decode_status(parsed.payload)
                    if status.transfer_id != manifest.transfer_id:
                        continue
                    if status.state == TransferState.COMPLETE:
                        completed = True
                        break
                    if status.state == TransferState.HASH_MISMATCH:
                        completed = False
                        break
                    # Treat an incomplete STATUS as a hint but require explicit repair request.
                    continue
                if parsed.frame_type != FrameType.REPAIR_REQUEST:
                    continue
                request = decode_repair_request(parsed.payload)
                if request.transfer_id != manifest.transfer_id:
                    continue
                indexes = expand_ranges(request.missing_ranges)
                if not indexes:
                    sock.sendto(encode_repair_done(manifest.transfer_id), response_addr)
                    completed = True
                    break
                for chunk_index in indexes:
                    if chunk_index >= len(chunks):
                        continue
                    sock.sendto(
                        encode_data_chunk(manifest.transfer_id, chunk_index, chunks[chunk_index]),
                        destination,
                    )
                    repaired_chunks += 1
                    if self.config.inter_packet_delay_s > 0:
                        time.sleep(self.config.inter_packet_delay_s)
                sock.sendto(encode_repair_done(manifest.transfer_id), destination)
                repair_rounds += 1

        return SendResult(
            transfer_id_hex=manifest.transfer_id.hex(),
            total_chunks=math.ceil(len(raw) / self.config.chunk_size) if raw else 0,
            repaired_chunks=repaired_chunks,
            repair_rounds=repair_rounds,
            completed=completed,
        )

