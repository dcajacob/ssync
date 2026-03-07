from __future__ import annotations

import socket
import time
from pathlib import Path

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

