#!/usr/bin/env python3
"""Run Python↔Rust ssync interoperability checks.

Covers both directions in:
- no-feedback mode
- feedback mode
- optional feedback+loss mode via a small UDP drop proxy

Examples:
  uv run python scripts/test_python_rust_interop.py
  uv run python scripts/test_python_rust_interop.py --with-loss
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

PY_REPO = Path("/home/dan/GIT/ssync")
RS_REPO = Path("/home/dan/GIT/ssync-rust")
RS_BIN = RS_REPO / "target" / "debug" / "ssync"
CARGO = Path("/home/dan/.cargo/bin/cargo")
UV = shutil.which("uv") or "uv"

sys.path.insert(0, str(PY_REPO / "src"))
from ssync.space_sync.frames import decode_frame  # type: ignore
from ssync.space_sync.types import FrameType  # type: ignore


@dataclass(slots=True)
class CaseResult:
    name: str
    ok: bool
    feedback: bool
    loss: bool
    sender_rc: int
    sender_stdout: str
    sender_stderr: str
    receiver_log: str = ""
    notes: str = ""


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def wait_for_file(path: Path, expected_sha: str, timeout: float = 15.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if path.exists() and sha256(path) == expected_sha:
            return True
        time.sleep(0.1)
    return False


class DropProxy:
    def __init__(self, listen_port: int, receiver_port: int, drop_every_n_data: int) -> None:
        self.listen_port = listen_port
        self.receiver_addr = ("127.0.0.1", receiver_port)
        self.drop_every_n_data = drop_every_n_data
        self.sender_addr: tuple[str, int] | None = None
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.forwarded_data = 0
        self.dropped_data = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", listen_port))
        self.sock.settimeout(0.2)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)
        self.sock.close()

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                payload, addr = self.sock.recvfrom(65535)
            except TimeoutError:
                continue
            except OSError:
                break

            if addr == self.receiver_addr:
                if self.sender_addr is not None:
                    self.sock.sendto(payload, self.sender_addr)
                continue

            self.sender_addr = addr
            drop = False
            try:
                frame = decode_frame(payload)
                if frame.frame_type == FrameType.DATA:
                    self.forwarded_data += 1
                    if (
                        self.drop_every_n_data > 0
                        and self.forwarded_data % self.drop_every_n_data == 0
                    ):
                        self.dropped_data += 1
                        drop = True
            except Exception:
                pass

            if not drop:
                self.sock.sendto(payload, self.receiver_addr)


def start_receiver(cmd: list[str], cwd: Path, log_path: Path) -> tuple[subprocess.Popen[bytes], object]:
    log = log_path.open("wb")
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=log,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )
    time.sleep(1.0)
    return proc, log


def stop_proc(proc: subprocess.Popen[bytes], log: object) -> None:
    try:
        if proc.poll() is None:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait(timeout=3)
    finally:
        log.close()


def build_rust() -> None:
    subprocess.run([str(CARGO), "build", "--quiet"], cwd=str(RS_REPO), check=True)


def python_send_cmd(src: Path, port: int, feedback: bool) -> list[str]:
    cmd = [
        UV,
        "run",
        "ssync",
        "send",
        str(src),
        "--dest-host",
        "127.0.0.1",
        "--dest-port",
        str(port),
        "--chunk-size",
        "4096",
        "--log-level",
        "INFO",
    ]
    if feedback:
        cmd.append("--feedback")
    else:
        cmd.extend(["--no-feedback", "--inter-packet-delay-s", "0.0001"])
    return cmd


def python_receive_cmd(out_dir: Path, port: int, feedback: bool) -> list[str]:
    cmd = [
        UV,
        "run",
        "ssync",
        "receive",
        "--bind-host",
        "127.0.0.1",
        "--bind-port",
        str(port),
        "--output-dir",
        str(out_dir),
        "--log-level",
        "INFO",
    ]
    if feedback:
        cmd.append("--feedback")
    return cmd


def rust_send_cmd(src: Path, port: int, feedback: bool) -> list[str]:
    cmd = [
        str(RS_BIN),
        "send",
        str(src),
        "--dest-host",
        "127.0.0.1",
        "--dest-port",
        str(port),
        "--chunk-size",
        "4096",
    ]
    if feedback:
        cmd.append("--feedback")
    return cmd


def rust_receive_cmd(out_dir: Path, port: int, feedback: bool) -> list[str]:
    cmd = [
        str(RS_BIN),
        "receive",
        "--bind-host",
        "127.0.0.1",
        "--bind-port",
        str(port),
        "--output-dir",
        str(out_dir),
    ]
    if feedback:
        cmd.append("--feedback")
    return cmd


def run_case(
    *,
    name: str,
    sender_cmd: list[str],
    sender_cwd: Path,
    receiver_cmd: list[str],
    receiver_cwd: Path,
    expected_file: Path,
    expected_sha: str,
    feedback: bool,
    loss: bool,
    notes: str = "",
) -> CaseResult:
    log_path = expected_file.parent.parent / f"{name}.receiver.log"
    receiver, log = start_receiver(receiver_cmd, receiver_cwd, log_path)
    try:
        sender = subprocess.run(
            sender_cmd,
            cwd=str(sender_cwd),
            capture_output=True,
            text=True,
            timeout=40,
        )
        ok = sender.returncode == 0 and wait_for_file(expected_file, expected_sha)
        receiver_log = log_path.read_text(errors="replace") if log_path.exists() else ""
        return CaseResult(
            name=name,
            ok=ok,
            feedback=feedback,
            loss=loss,
            sender_rc=sender.returncode,
            sender_stdout=sender.stdout.strip(),
            sender_stderr=sender.stderr.strip(),
            receiver_log=receiver_log.strip(),
            notes=notes,
        )
    finally:
        stop_proc(receiver, log)


def print_result(result: CaseResult) -> None:
    print(
        f"CASE {result.name} ok={result.ok} feedback={result.feedback} "
        f"loss={result.loss} sender_rc={result.sender_rc}"
    )
    if result.notes:
        print(f"NOTES {result.notes}")
    if result.sender_stdout:
        print("SENDER_STDOUT>>>")
        print(result.sender_stdout)
    if result.sender_stderr:
        print("SENDER_STDERR>>>")
        print(result.sender_stderr)
    if not result.ok and result.receiver_log:
        print("RECEIVER_LOG>>>")
        print(result.receiver_log)
    print("---")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Python↔Rust ssync interoperability tests")
    parser.add_argument("--with-loss", action="store_true", help="also run feedback tests through a lossy UDP proxy")
    parser.add_argument("--drop-every-n-data", type=int, default=5, help="drop cadence for --with-loss (default: 5)")
    args = parser.parse_args()

    build_rust()

    with tempfile.TemporaryDirectory(prefix="ssync-interop-") as td:
        root = Path(td)
        src = root / "payload.bin"
        src.write_bytes(os.urandom(512 * 1024 + 137))
        expected_sha = sha256(src)

        results: list[CaseResult] = []

        base_cases = [
            (
                "py_to_rs_open",
                python_send_cmd,
                PY_REPO,
                rust_receive_cmd,
                RS_REPO,
                False,
                "Python open-loop send uses --inter-packet-delay-s 0.0001 for stable localhost delivery to Rust.",
            ),
            (
                "rs_to_py_open",
                rust_send_cmd,
                RS_REPO,
                python_receive_cmd,
                PY_REPO,
                False,
                "",
            ),
            (
                "py_to_rs_feedback",
                python_send_cmd,
                PY_REPO,
                rust_receive_cmd,
                RS_REPO,
                True,
                "",
            ),
            (
                "rs_to_py_feedback",
                rust_send_cmd,
                RS_REPO,
                python_receive_cmd,
                PY_REPO,
                True,
                "",
            ),
        ]

        for name, send_fn, send_cwd, recv_fn, recv_cwd, feedback, notes in base_cases:
            out_dir = root / name
            out_dir.mkdir()
            port = free_port()
            results.append(
                run_case(
                    name=name,
                    sender_cmd=send_fn(src, port, feedback),
                    sender_cwd=send_cwd,
                    receiver_cmd=recv_fn(out_dir, port, feedback),
                    receiver_cwd=recv_cwd,
                    expected_file=out_dir / src.name,
                    expected_sha=expected_sha,
                    feedback=feedback,
                    loss=False,
                    notes=notes,
                )
            )

        if args.with_loss:
            loss_cases = [
                (
                    "py_to_rs_feedback_loss",
                    python_send_cmd,
                    PY_REPO,
                    rust_receive_cmd,
                    RS_REPO,
                ),
                (
                    "rs_to_py_feedback_loss",
                    rust_send_cmd,
                    RS_REPO,
                    python_receive_cmd,
                    PY_REPO,
                ),
            ]
            for name, send_fn, send_cwd, recv_fn, recv_cwd in loss_cases:
                out_dir = root / name
                out_dir.mkdir()
                recv_port = free_port()
                proxy_port = free_port()
                proxy = DropProxy(proxy_port, recv_port, args.drop_every_n_data)
                receiver_cmd = recv_fn(out_dir, recv_port, True)
                log_path = root / f"{name}.receiver.log"
                receiver, log = start_receiver(receiver_cmd, recv_cwd, log_path)
                proxy.start()
                try:
                    sender = subprocess.run(
                        send_fn(src, proxy_port, True),
                        cwd=str(send_cwd),
                        capture_output=True,
                        text=True,
                        timeout=40,
                    )
                    ok = sender.returncode == 0 and wait_for_file(out_dir / src.name, expected_sha)
                    results.append(
                        CaseResult(
                            name=name,
                            ok=ok,
                            feedback=True,
                            loss=True,
                            sender_rc=sender.returncode,
                            sender_stdout=sender.stdout.strip(),
                            sender_stderr=sender.stderr.strip(),
                            receiver_log=log_path.read_text(errors="replace").strip() if log_path.exists() else "",
                            notes=(
                                f"UDP proxy dropped every {args.drop_every_n_data}th DATA packet; "
                                f"proxy dropped {proxy.dropped_data} data packets."
                            ),
                        )
                    )
                finally:
                    proxy.stop()
                    stop_proc(receiver, log)

        for result in results:
            print_result(result)

        return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
