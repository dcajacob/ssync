from __future__ import annotations

import hashlib
import socket
import threading
from dataclasses import dataclass
from pathlib import Path

from .frames import (
    TransferStatus,
    decode_data_chunk,
    decode_fin,
    decode_frame,
    decode_manifest,
    decode_repair_done,
    encode_repair_request,
    encode_status,
)
from .manifest import RepairRequest, TransferManifest
from .ranges import ChunkTracker
from .types import (
    DEFAULT_SOCKET_TIMEOUT,
    FrameType,
    ReceivedTransferInfo,
    ReceiverConfig,
    TransferState,
)


@dataclass(slots=True)
class _TransferStateData:
    manifest: TransferManifest
    part_path: Path
    final_path: Path
    tracker: ChunkTracker
    source_addr: tuple[str, int]
    done: bool = False
    hash_mismatch: bool = False


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
        self._lock = threading.Lock()
        self._transfers: dict[bytes, _TransferStateData] = {}
        self._completed: list[ReceivedTransferInfo] = []
        self._thread: threading.Thread | None = None

    @property
    def completed_transfers(self) -> list[ReceivedTransferInfo]:
        with self._lock:
            return list(self._completed)

    def start(self) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def run(self) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind((self.bind_host, self.bind_port))
            sock.settimeout(DEFAULT_SOCKET_TIMEOUT)
            while not self._stop_event.is_set():
                try:
                    raw, source_addr = sock.recvfrom(65535)
                except TimeoutError:
                    continue
                try:
                    frame = decode_frame(raw)
                except ValueError:
                    continue
                self._handle_frame(sock, frame.frame_type, frame.payload, source_addr)

    def _handle_frame(
        self,
        sock: socket.socket,
        frame_type: FrameType,
        payload: bytes,
        source_addr: tuple[str, int],
    ) -> None:
        if frame_type == FrameType.MANIFEST:
            manifest = decode_manifest(payload)
            self._prepare_transfer(manifest, source_addr)
            return
        if frame_type == FrameType.DATA:
            chunk = decode_data_chunk(payload)
            self._accept_data(chunk.transfer_id, chunk.chunk_index, chunk.payload)
            return
        if frame_type == FrameType.FIN:
            transfer_id = decode_fin(payload)
            self._on_fin(sock, transfer_id)
            return
        if frame_type == FrameType.REPAIR_DONE:
            transfer_id = decode_repair_done(payload)
            self._on_repair_done(sock, transfer_id)

    def _prepare_transfer(self, manifest: TransferManifest, source_addr: tuple[str, int]) -> None:
        final_path = self._safe_destination_path(manifest.file_name)
        if final_path is None:
            return
        final_path.parent.mkdir(parents=True, exist_ok=True)
        part_path = self.config.output_dir / f".{manifest.transfer_id.hex()}.part"
        with self._lock:
            if manifest.transfer_id in self._transfers:
                return
            with part_path.open("wb") as stream:
                stream.truncate(manifest.file_size)
            self._transfers[manifest.transfer_id] = _TransferStateData(
                manifest=manifest,
                part_path=part_path,
                final_path=final_path,
                tracker=ChunkTracker(total_chunks=manifest.total_chunks),
                source_addr=source_addr,
            )

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

    def _accept_data(self, transfer_id: bytes, chunk_index: int, payload: bytes) -> None:
        with self._lock:
            transfer = self._transfers.get(transfer_id)
        if transfer is None or transfer.done:
            return
        if chunk_index >= transfer.manifest.total_chunks:
            return
        chunk_start = chunk_index * transfer.manifest.chunk_size
        if chunk_start + len(payload) > transfer.manifest.file_size:
            return
        with transfer.part_path.open("r+b") as stream:
            stream.seek(chunk_start)
            stream.write(payload)
        transfer.tracker.add(chunk_index)

    def _on_fin(self, sock: socket.socket, transfer_id: bytes) -> None:
        with self._lock:
            transfer = self._transfers.get(transfer_id)
        if transfer is None:
            return
        missing_ranges = transfer.tracker.missing_ranges()
        if missing_ranges and self.config.enable_feedback:
            request = RepairRequest(transfer_id=transfer_id, missing_ranges=missing_ranges)
            sock.sendto(encode_repair_request(request), transfer.source_addr)
            status = ReceivedTransferInfo(
                transfer_id_hex=transfer_id.hex(),
                file_name=transfer.manifest.file_name,
                completed=False,
                missing_ranges=missing_ranges,
            )
            with self._lock:
                self._completed.append(status)
            return
        self._finalize_transfer(sock, transfer, missing_ranges)

    def _on_repair_done(self, sock: socket.socket, transfer_id: bytes) -> None:
        with self._lock:
            transfer = self._transfers.get(transfer_id)
        if transfer is None or transfer.done:
            return
        self._finalize_transfer(sock, transfer, transfer.tracker.missing_ranges())

    def _finalize_transfer(
        self,
        sock: socket.socket,
        transfer: _TransferStateData,
        missing_ranges: list[tuple[int, int]],
    ) -> None:
        state = TransferState.INCOMPLETE
        hash_mismatch = False
        if not missing_ranges:
            actual_hash = hashlib.sha256(transfer.part_path.read_bytes()).digest()
            if actual_hash == transfer.manifest.sha256:
                state = TransferState.COMPLETE
                transfer.part_path.replace(transfer.final_path)
            else:
                hash_mismatch = True
                state = TransferState.HASH_MISMATCH
        transfer.done = (state == TransferState.COMPLETE)
        if self.config.enable_feedback:
            for _ in range(self.config.status_repeat):
                sock.sendto(
                    encode_status(
                        TransferStatus(
                            transfer_id=transfer.manifest.transfer_id,
                            state=state,
                            missing_ranges=missing_ranges,
                        )
                    ),
                    transfer.source_addr,
                )
        info = ReceivedTransferInfo(
            transfer_id_hex=transfer.manifest.transfer_id.hex(),
            file_name=transfer.manifest.file_name,
            completed=(state == TransferState.COMPLETE),
            missing_ranges=missing_ranges,
            hash_mismatch=hash_mismatch,
        )
        with self._lock:
            self._completed.append(info)

