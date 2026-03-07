from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
from dataclasses import dataclass
from pathlib import Path

from .frames import (
    TransferStatus,
    decode_data_chunk,
    decode_file_info_request,
    decode_fin,
    decode_frame,
    decode_manifest,
    decode_repair_done,
    encode_file_info_response,
    encode_repair_request,
    encode_status,
)
from .manifest import RepairRequest, TransferManifest
from .ranges import ChunkTracker
from .types import (
    DEFAULT_SOCKET_TIMEOUT,
    TRANSFER_ID_SIZE,
    FrameType,
    MetadataType,
    ReceivedTransferInfo,
    ReceiverConfig,
    RemoteFileInfo,
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
        self._load_journal()
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        with self._lock:
            self._save_journal_locked()

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
                if frame.frame_type is None:
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
            try:
                manifest = decode_manifest(payload)
            except ValueError:
                return
            self._prepare_transfer(manifest, source_addr)
            return
        if frame_type == FrameType.DATA:
            try:
                chunk = decode_data_chunk(payload)
            except ValueError:
                return
            self._accept_data(chunk.transfer_id, chunk.chunk_index, chunk.payload)
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
            sock.sendto(encode_file_info_response(info), source_addr)

    def _prepare_transfer(self, manifest: TransferManifest, source_addr: tuple[str, int]) -> None:
        final_path = self._safe_destination_path(manifest.file_name)
        if final_path is None:
            return
        final_path.parent.mkdir(parents=True, exist_ok=True)
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
            )
            self._save_journal_locked()

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
        changed = transfer.tracker.add(chunk_index)
        if changed:
            with self._lock:
                self._save_journal_locked()

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
                source_mtime = transfer.manifest.metadata.get(int(MetadataType.SOURCE_MTIME_NS))
                if source_mtime is not None and len(source_mtime) == 8:
                    mtime_ns = int.from_bytes(source_mtime, "big")
                    os.utime(transfer.final_path, ns=(mtime_ns, mtime_ns))
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
            self._transfers.pop(transfer.manifest.transfer_id, None)
            self._save_journal_locked()

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
                    "source_addr": [transfer.source_addr[0], transfer.source_addr[1]],
                }
            )
        journal_path = self._journal_path()
        temp_path = journal_path.with_suffix(".json.tmp")
        temp_path.write_text(json.dumps({"transfers": records}, indent=2), encoding="utf-8")
        temp_path.replace(journal_path)

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
            self._save_journal_locked()

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
        return _TransferStateData(
            manifest=manifest,
            part_path=part_path,
            final_path=final_path,
            tracker=tracker,
            source_addr=source_addr,
        )

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

