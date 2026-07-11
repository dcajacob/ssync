from __future__ import annotations

import json
import socket
import subprocess
import sys
from pathlib import Path


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "from ssync.space_sync.cli import main; raise SystemExit(main())",
            *args,
        ],
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=10,
    )


def test_send_json_contract_open_loop_subprocess(tmp_path: Path) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"subprocess-open-loop")

    result = _run_cli(
        "send",
        str(source),
        "--dest-host",
        "127.0.0.1",
        "--dest-port",
        str(_free_udp_port()),
        "--no-feedback",
        "--json",
        cwd=tmp_path,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 1
    assert payload["summary"]["success"] is True
    assert payload["results"][0]["outcome"] == "open_loop_sent"
    assert payload["results"][0]["transmission_complete"] is True


def test_send_feedback_failure_exits_nonzero_subprocess(tmp_path: Path) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"subprocess-feedback-failure")

    result = _run_cli(
        "send",
        str(source),
        "--dest-host",
        "127.0.0.1",
        "--dest-port",
        str(_free_udp_port()),
        "--feedback",
        "--feedback-wait-s",
        "0.05",
        "--max-feedback-idle-timeouts",
        "1",
        "--max-repair-rounds",
        "1",
        "--json",
        cwd=tmp_path,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["summary"]["success"] is False
    assert payload["results"][0]["outcome"] == "incomplete"


def test_sync_dry_run_json_contract_subprocess(tmp_path: Path) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"dry-run")

    result = _run_cli(
        "-n",
        str(source),
        "127.0.0.1:payload.bin",
        "--json",
        cwd=tmp_path,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 1
    assert payload["summary"]["would_send"] == 1
    assert payload["results"][0]["status"] == "would-send"
