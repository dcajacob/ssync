from __future__ import annotations

import socket
import threading
import time
from pathlib import Path

import pytest

from ssync.space_sync import sender as sender_module
from ssync.space_sync.frames import encode_file_info_response
from ssync.space_sync.manifest import TransferManifest
from ssync.space_sync.sender import SpaceSyncSender
from ssync.space_sync.types import RemoteFileInfo, SenderConfig


def test_apply_rate_limit_treats_bps_as_bits_per_second(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender = SpaceSyncSender(SenderConfig(max_data_rate_bps=8_000_000))
    sleeps: list[float] = []

    def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(sender_module.time, "monotonic", lambda: 10.25)
    monkeypatch.setattr(sender_module.time, "sleep", _fake_sleep)

    paced_total = sender._apply_rate_limit(
        paced_start_s=10.0,
        paced_data_bytes=0,
        just_sent_bytes=1_000_000,
    )

    assert paced_total == 1_000_000
    assert sleeps == [pytest.approx(0.75)]


def test_send_file_uses_single_read_for_manifest(tmp_path: Path) -> None:
    source_path = tmp_path / "payload.bin"
    source_payload = b"single-read-check" * 256
    source_path.write_bytes(source_payload)
    sender = SpaceSyncSender(SenderConfig(enable_feedback=False))

    original_from_bytes = TransferManifest.from_bytes
    captured_raw: list[bytes] = []

    def _wrapped_from_bytes(
        *,
        raw: bytes,
        file_name: str,
        chunk_size: int,
        metadata: dict[int, bytes] | None = None,
    ) -> TransferManifest:
        captured_raw.append(raw)
        return original_from_bytes(
            raw=raw,
            file_name=file_name,
            chunk_size=chunk_size,
            metadata=metadata,
        )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sender_module.TransferManifest, "from_bytes", _wrapped_from_bytes)
    try:
        sender.send_file(source_path, "127.0.0.1", 9_999)
    finally:
        monkeypatch.undo()

    assert captured_raw == [source_payload]


def test_query_remote_file_times_out_on_non_matching_responses() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    host, port = server.getsockname()
    stop_event = threading.Event()

    def _spam_wrong_path() -> None:
        try:
            request_raw, addr = server.recvfrom(65535)
            assert request_raw
            wrong = encode_file_info_response(RemoteFileInfo(path="wrong-name.bin", exists=False))
            while not stop_event.is_set():
                server.sendto(wrong, addr)
                time.sleep(0.01)
        finally:
            server.close()

    worker = threading.Thread(target=_spam_wrong_path, daemon=True)
    worker.start()
    sender = SpaceSyncSender(SenderConfig(enable_feedback=True, feedback_wait_s=0.2))
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        sender.query_remote_file(
            destination_host=str(host),
            destination_port=int(port),
            remote_name="expected-name.bin",
            include_checksum=False,
        )
    elapsed = time.monotonic() - start
    stop_event.set()
    worker.join(timeout=1.0)
    assert elapsed < 1.0
