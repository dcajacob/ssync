from __future__ import annotations

import json
import socket
import time
from pathlib import Path

from ssync.space_sync.frames import (
    encode_data_chunk,
    encode_fin,
    encode_manifest,
    encode_repair_done,
)
from ssync.space_sync.manifest import TransferManifest
from ssync.space_sync.receiver import SpaceSyncReceiver
from ssync.space_sync.sender import SpaceSyncSender
from ssync.space_sync.types import ReceiverConfig, SenderConfig


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_file(path: Path, timeout_s: float = 3.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


def _wait_for_predicate(predicate: object, timeout_s: float = 3.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if callable(predicate) and predicate():
            return True
        time.sleep(0.05)
    return False


def test_open_loop_local_transfer(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-open"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=False),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-open.bin"
        source_payload = b"abc123" * 500
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(chunk_size=256, enable_feedback=False),
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.total_chunks > 0

        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_feedback_repair_transfer(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-repair"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-repair.bin"
        source_payload = b"0123456789abcdef" * 1000
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=128,
                enable_feedback=True,
                drop_every_nth_data=4,
                max_repair_rounds=3,
                feedback_wait_s=3.0,
            )
        )
        result = sender.send_file(source_path, "127.0.0.1", receiver.bind_port)
        assert result.repaired_chunks > 0

        target_path = receiver_dir / source_path.name
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_transfer_to_remote_subpath(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-subpath"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=False),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-subpath.bin"
        source_payload = b"xyz987" * 400
        source_path.write_bytes(source_payload)

        sender = SpaceSyncSender(
            config=SenderConfig(chunk_size=256, enable_feedback=False),
        )
        result = sender.send_file(
            source_path,
            "127.0.0.1",
            receiver.bind_port,
            remote_name="nested/dir/final.bin",
        )
        assert result.total_chunks > 0

        target_path = receiver_dir / "nested" / "dir" / "final.bin"
        assert _wait_for_file(target_path)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver.stop()


def test_feedback_mode_times_out_without_receiver(tmp_path: Path) -> None:
    source_path = tmp_path / "no-receiver.bin"
    source_path.write_bytes(b"abcdef" * 1024)

    sender = SpaceSyncSender(
        config=SenderConfig(
            chunk_size=256,
            enable_feedback=True,
            feedback_wait_s=0.1,
            max_feedback_idle_timeouts=1,
            max_repair_rounds=3,
        )
    )
    start = time.monotonic()
    result = sender.send_file(source_path, "127.0.0.1", _free_udp_port())
    elapsed = time.monotonic() - start

    assert result.completed is False
    assert result.repair_rounds == 0
    assert elapsed < 1.0


def test_remote_file_query_and_unchanged_detection(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-query"
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=_free_udp_port(),
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        source_path = tmp_path / "source-query.bin"
        source_payload = b"remote-query-" * 600
        source_path.write_bytes(source_payload)
        sender = SpaceSyncSender(
            config=SenderConfig(chunk_size=256, enable_feedback=True, feedback_wait_s=2.0),
        )
        send_result = sender.send_file(
            source_path,
            "127.0.0.1",
            receiver.bind_port,
            remote_name="nested/query.bin",
        )
        assert send_result.completed is True
        info = sender.query_remote_file(
            destination_host="127.0.0.1",
            destination_port=receiver.bind_port,
            remote_name="nested/query.bin",
            include_checksum=True,
        )
        assert info.exists is True
        assert info.size == len(source_payload)
        assert info.sha256 == SpaceSyncSender.local_file_checksum(source_path)
        assert info.mtime_ns == source_path.stat().st_mtime_ns
    finally:
        receiver.stop()


def test_receiver_recovers_incomplete_transfer_after_restart(tmp_path: Path) -> None:
    receiver_dir = tmp_path / "rx-restart"
    bind_port = _free_udp_port()
    source_path = tmp_path / "source-restart.bin"
    source_payload = b"restart-check-" * 300
    source_path.write_bytes(source_payload)

    manifest = TransferManifest.from_file(source_path, chunk_size=128, remote_name="resumed.bin")
    manifest.transfer_id = b"\x99" * 16
    chunks = [
        source_payload[offset : offset + manifest.chunk_size]
        for offset in range(0, len(source_payload), manifest.chunk_size)
    ]
    missing_chunk_index = 2

    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=bind_port,
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver.start()
    try:
        time.sleep(0.1)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            destination = ("127.0.0.1", bind_port)
            sock.sendto(encode_manifest(manifest), destination)
            for chunk_index, payload in enumerate(chunks):
                if chunk_index == missing_chunk_index:
                    continue
                sock.sendto(
                    encode_data_chunk(manifest.transfer_id, chunk_index, payload),
                    destination,
                )
            sock.sendto(encode_fin(manifest.transfer_id), destination)
        time.sleep(0.3)
    finally:
        receiver.stop()

    journal_path = receiver_dir / ".ssync-journal.json"
    assert journal_path.exists()
    journal_raw = json.loads(journal_path.read_text(encoding="utf-8"))
    assert "transfers" in journal_raw

    receiver2 = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=bind_port,
        config=ReceiverConfig(output_dir=receiver_dir, enable_feedback=True),
    )
    receiver2.start()
    try:
        time.sleep(0.1)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            destination = ("127.0.0.1", bind_port)
            sock.sendto(
                encode_data_chunk(
                    manifest.transfer_id,
                    missing_chunk_index,
                    chunks[missing_chunk_index],
                ),
                destination,
            )
            sock.sendto(encode_repair_done(manifest.transfer_id), destination)
        target_path = receiver_dir / "resumed.bin"
        assert _wait_for_file(target_path, timeout_s=4.0)
        assert target_path.read_bytes() == source_payload
    finally:
        receiver2.stop()

