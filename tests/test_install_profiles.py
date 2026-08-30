from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from ssync.space_sync import cli

_SOURCE_DIR = Path(__file__).resolve().parents[1] / "src"


def _run_without_site_packages(
    tmp_path: Path, entrypoint: str, args: list[str]
) -> subprocess.CompletedProcess[str]:
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(_SOURCE_DIR)!r})\n"
        "from ssync.space_sync import cli\n"
        "assert 'rich' not in sys.modules\n"
        "assert 'ssync.space_sync.monitor' not in sys.modules\n"
        "raise SystemExit(getattr(cli, sys.argv[1])(sys.argv[2:]))\n"
    )
    return subprocess.run(
        [sys.executable, "-I", "-S", "-c", code, entrypoint, *args],
        cwd=tmp_path,
        env={**os.environ, "HOME": str(tmp_path), "XDG_CONFIG_HOME": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


@pytest.mark.parametrize(
    ("entrypoint", "args"),
    [
        ("main", ["--help"]),
        ("main", ["send", "--help"]),
        ("main", ["receive", "--help"]),
        ("main", ["server", "--help"]),
        ("main", ["ssyncd", "--help"]),
        ("main", ["monitor", "--help"]),
        ("ssyncd_main", ["--help"]),
    ],
)
def test_cli_help_without_third_party_packages(
    tmp_path: Path, entrypoint: str, args: list[str]
) -> None:
    result = _run_without_site_packages(tmp_path, entrypoint, args)
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout


def test_space_sync_dry_run_without_third_party_packages(tmp_path: Path) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"space payload")
    result = _run_without_site_packages(
        tmp_path, "main", ["--dry-run", str(source), "127.0.0.1:payload.bin"]
    )
    assert result.returncode == 0, result.stderr
    assert "would_send=1" in result.stdout


def test_monitor_without_rich_reports_ground_extra(tmp_path: Path) -> None:
    output_dir = tmp_path / "received"
    result = _run_without_site_packages(
        tmp_path, "main", ["monitor", "--output-dir", str(output_dir)]
    )
    assert result.returncode == 2
    assert "Rich is required" in result.stderr
    assert "ssync[ground]" in result.stderr
    assert "Traceback" not in result.stderr
    assert not output_dir.exists()


@pytest.mark.parametrize("ipc_path", [None, Path("custom.sock")])
def test_ground_monitor_dispatch(
    monkeypatch: pytest.MonkeyPatch, ipc_path: Path | None
) -> None:
    from ssync.space_sync import monitor

    def fake_monitor(
        output_dir: Path, refresh_interval_s: float, monitor_ipc_socket: Path | None
    ) -> int:
        assert output_dir == Path("received")
        assert refresh_interval_s == 0.2
        assert monitor_ipc_socket == (ipc_path or Path("received/.ssync-monitor.sock"))
        return 0

    monkeypatch.setattr(monitor, "run_monitor_tui", fake_monitor)
    args = cli._build_parser().parse_args(["monitor", "--refresh-interval-s", "0.2"])
    args.monitor_ipc_socket = ipc_path
    assert cli._run_monitor(args) == 0


def test_ground_monitor_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupted_monitor(**kwargs: object) -> int:
        raise KeyboardInterrupt

    monkeypatch.setitem(
        sys.modules,
        "ssync.space_sync.monitor",
        SimpleNamespace(run_monitor_tui=interrupted_monitor),
    )
    args = cli._build_parser().parse_args(["monitor"])
    assert cli._run_monitor(args) == 0


def test_monitor_does_not_hide_unrelated_import_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "ssync.space_sync.monitor", None)
    args = cli._build_parser().parse_args(["monitor"])
    with pytest.raises(ModuleNotFoundError) as exc_info:
        cli._run_monitor(args)
    assert exc_info.value.name == "ssync.space_sync.monitor"
