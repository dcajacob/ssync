#!/usr/bin/env python3
# pyright: reportMissingImports=false
from __future__ import annotations

import argparse
import hashlib
import socket
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ssync.space_sync.receiver import SpaceSyncReceiver
from ssync.space_sync.sender import SpaceSyncSender
from ssync.space_sync.types import ReceiverConfig, SenderConfig


@dataclass(slots=True)
class ScenarioResult:
    name: str
    passed: bool
    details: str


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_file(path: Path, timeout_s: float = 5.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run_open_loop(temp_dir: Path, chunk_size: int) -> ScenarioResult:
    name = "open-loop"
    rx_dir = temp_dir / "rx-open"
    source = temp_dir / "source-open.bin"
    source.write_bytes((b"space-sync-open-loop-" * 4096))

    port = _free_udp_port()
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=port,
        config=ReceiverConfig(output_dir=rx_dir, enable_feedback=False),
    )
    receiver.start()
    # Receiver starts on a background thread. A short warmup avoids a race where
    # the sender transmits before the UDP socket has bound.
    time.sleep(0.15)
    try:
        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=chunk_size,
                enable_feedback=False,
                manifest_repeats=5,
                inter_packet_delay_s=0.0005,
            ),
        )
        target = rx_dir / source.name
        last_details = "receiver did not produce output file"
        for attempt in range(1, 4):
            if target.exists():
                target.unlink()
            send_result = sender.send_file(source, "127.0.0.1", port)
            if not _wait_for_file(target, timeout_s=8.0):
                last_details = f"attempt {attempt}: receiver did not produce output file"
                continue
            if _sha256(source) != _sha256(target):
                last_details = f"attempt {attempt}: hash mismatch after open-loop transfer"
                continue
            return ScenarioResult(
                name,
                True,
                (
                    f"attempt={attempt} chunks={send_result.total_chunks} "
                    f"transfer_id={send_result.transfer_id_hex}"
                ),
            )
        return ScenarioResult(name, False, last_details)
    finally:
        receiver.stop()


def _run_feedback_repair(
    temp_dir: Path,
    chunk_size: int,
    drop_every_nth_data: int,
) -> ScenarioResult:
    name = "feedback-repair"
    rx_dir = temp_dir / "rx-repair"
    source = temp_dir / "source-repair.bin"
    source.write_bytes((b"space-sync-feedback-repair-" * 4096))

    port = _free_udp_port()
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=port,
        config=ReceiverConfig(output_dir=rx_dir, enable_feedback=True),
    )
    receiver.start()
    # Keep behavior consistent with open-loop startup sequencing.
    time.sleep(0.15)
    try:
        sender = SpaceSyncSender(
            config=SenderConfig(
                chunk_size=chunk_size,
                enable_feedback=True,
                drop_every_nth_data=drop_every_nth_data,
                max_repair_rounds=3,
                feedback_wait_s=4.0,
            ),
        )
        send_result = sender.send_file(source, "127.0.0.1", port)
        target = rx_dir / source.name
        if not _wait_for_file(target):
            return ScenarioResult(name, False, "receiver did not produce repaired output file")
        if _sha256(source) != _sha256(target):
            return ScenarioResult(name, False, "hash mismatch after feedback repair transfer")
        if send_result.repaired_chunks <= 0:
            return ScenarioResult(name, False, "no chunks repaired; induced loss may not have triggered")
        return ScenarioResult(
            name,
            True,
            (
                "chunks="
                f"{send_result.total_chunks} repaired={send_result.repaired_chunks} "
                f"rounds={send_result.repair_rounds} transfer_id={send_result.transfer_id_hex}"
            ),
        )
    finally:
        receiver.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Space Sync loopback integration scenarios")
    parser.add_argument("--chunk-size", type=int, default=256)
    parser.add_argument(
        "--drop-every-nth-data",
        type=int,
        default=5,
        help="Induced loss cadence for feedback-repair scenario",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="ssync-loopback-") as temp_root:
        temp_dir = Path(temp_root)
        results = [
            _run_open_loop(temp_dir=temp_dir, chunk_size=args.chunk_size),
            _run_feedback_repair(
                temp_dir=temp_dir,
                chunk_size=args.chunk_size,
                drop_every_nth_data=max(2, args.drop_every_nth_data),
            ),
        ]

    print("Space Sync loopback scenarios")
    print("=" * 32)
    failed = False
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name}: {result.details}")
        if not result.passed:
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

