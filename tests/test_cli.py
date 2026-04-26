from __future__ import annotations

import argparse
import time
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from ssync.space_sync import cli as cli_module
from ssync.space_sync import config_file as config_file_module
from ssync.space_sync.cli import (
    _build_parser,
    _build_rsync_parser,
    _build_ssyncd_parser,
    _collect_sync_items,
    _is_unchanged,
    _load_open_loop_state,
    _order_items_for_open_loop,
    _parse_destination,
    _save_open_loop_state,
)
from ssync.space_sync.config_file import load_cli_config_defaults
from ssync.space_sync.types import RemoteFileInfo


def test_parse_destination_valid() -> None:
    host, remote = _parse_destination("127.0.0.1:dropzone/file.bin")
    assert host == "127.0.0.1"
    assert remote == "dropzone/file.bin"


def test_parse_destination_user_host_valid() -> None:
    host, remote = _parse_destination("dan@127.0.0.1:dropzone/file.bin")
    assert host == "127.0.0.1"
    assert remote == "dropzone/file.bin"


@pytest.mark.parametrize(
    "value",
    [
        "127.0.0.1",
        ":dropzone/file.bin",
        "127.0.0.1:",
        "127.0.0.1:/absolute/path",
        "dan@:dropzone/file.bin",
    ],
)
def test_parse_destination_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        _parse_destination(value)


def test_collect_sync_items_file_explicit_name(tmp_path: Path) -> None:
    source_file = tmp_path / "data.bin"
    source_file.write_bytes(b"abc")
    items = _collect_sync_items(
        source_file,
        "incoming/final.bin",
        recursive=False,
        includes=[],
        excludes=[],
    )
    assert items == [(source_file.resolve(), "incoming/final.bin")]


def test_collect_sync_items_file_directory_target(tmp_path: Path) -> None:
    source_file = tmp_path / "data.bin"
    source_file.write_bytes(b"abc")
    items = _collect_sync_items(
        source_file,
        "incoming/",
        recursive=False,
        includes=[],
        excludes=[],
    )
    assert items == [(source_file.resolve(), "incoming/data.bin")]


def test_collect_sync_items_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "payload"
    (source_dir / "a").mkdir(parents=True, exist_ok=True)
    (source_dir / "root.txt").write_text("root")
    (source_dir / "a" / "nested.txt").write_text("nested")
    items = _collect_sync_items(
        source_dir,
        "missions/pass-001/",
        recursive=True,
        includes=[],
        excludes=[],
    )
    remote_names = {remote_name for _, remote_name in items}
    assert remote_names == {
        "missions/pass-001/root.txt",
        "missions/pass-001/a/nested.txt",
    }


def test_sync_supports_json_flag() -> None:
    rsync_parser = _build_rsync_parser()
    sync_args = rsync_parser.parse_args(["src", "127.0.0.1:dst", "--json"])
    assert sync_args.json_output is True


def _subparsers_action(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise AssertionError("parser has no subparsers")


def test_cli_help_shows_common_options_and_hides_advanced() -> None:
    parser = _build_parser()
    subparsers = _subparsers_action(parser)
    assert "send" not in subparsers.choices
    assert "receive" not in subparsers.choices
    server_help = subparsers.choices["server"].format_help()
    assert "--bind-port" in server_help
    assert "--root-dir" in server_help
    assert "--status-repeat" not in server_help
    assert "--pre-metadata-ttl-s" not in server_help

    sync_help = _build_rsync_parser().format_help()
    assert "--open-loop-max-rounds" in sync_help
    assert "--skip-unchanged" in sync_help
    assert "--state-file" in sync_help
    assert "--manifest-repeats" not in sync_help
    assert "--max-repair-rounds" not in sync_help
    assert "--delete" not in sync_help


def test_hidden_advanced_flags_still_parse_sync_and_server() -> None:
    parser = _build_parser()
    server_ns = parser.parse_args(
        [
            "server",
            "--status-repeat",
            "5",
            "--pre-metadata-ttl-s",
            "12.5",
        ]
    )
    assert server_ns.status_repeat == 5
    assert server_ns.pre_metadata_ttl_s == pytest.approx(12.5)

    rsync_parser = _build_rsync_parser()
    sync_ns = rsync_parser.parse_args(
        [
            "src",
            "127.0.0.1:dst",
            "--feedback-wait-s",
            "1.5",
            "--repair-worker-poll-interval-s",
            "0.02",
        ]
    )
    assert sync_ns.feedback_wait_s == pytest.approx(1.5)
    assert sync_ns.repair_worker_poll_interval_s == pytest.approx(0.02)


def test_removed_send_receive_subcommands_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli_module.main(["send", "data.bin"]) == 2
    assert "removed" in capsys.readouterr().out
    assert cli_module.main(["receive", "--bind-port", "9000"]) == 2
    assert "removed" in capsys.readouterr().out


def test_parser_supports_metadata_alias_flags() -> None:
    parser = _build_parser()
    server_args = parser.parse_args(
        [
            "server",
            "--pre-metadata-max-pending-bytes",
            "123456",
            "--pre-metadata-max-pending-bytes-per-transfer",
            "2048",
            "--pre-metadata-max-pending-transfers",
            "16",
            "--pre-metadata-ttl-s",
            "12.5",
        ]
    )
    rsync_parser = _build_rsync_parser()
    sync_ns = rsync_parser.parse_args(
        [
            "src",
            "127.0.0.1:dst",
            "--metadata-repeats",
            "5",
            "--periodic-metadata-interval-s",
            "1.5",
            "--periodic-metadata-every-n-chunks",
            "100",
        ]
    )
    assert sync_ns.manifest_repeats == 5
    assert sync_ns.periodic_metadata_interval_s == pytest.approx(1.5)
    assert sync_ns.periodic_metadata_every_n_chunks == 100
    assert server_args.pre_metadata_max_pending_bytes == 123456
    assert server_args.pre_metadata_max_pending_bytes_per_transfer == 2048
    assert server_args.pre_metadata_max_pending_transfers == 16
    assert server_args.pre_metadata_ttl_s == pytest.approx(12.5)


def test_parser_supports_adaptive_leading_hole_repair_flags() -> None:
    parser = _build_parser()
    server_args = parser.parse_args(
        [
            "server",
            "--adaptive-leading-hole-boost",
            "--leading-hole-start-threshold-chunks",
            "128",
            "--leading-hole-min-span-chunks",
            "4096",
            "--leading-hole-boost-multiplier",
            "6",
            "--leading-hole-max-repair-chunks-per-request",
            "8192",
        ]
    )
    assert server_args.adaptive_leading_hole_boost is True
    assert server_args.leading_hole_start_threshold_chunks == 128
    assert server_args.leading_hole_min_span_chunks == 4096
    assert server_args.leading_hole_boost_multiplier == 6
    assert server_args.leading_hole_max_repair_chunks_per_request == 8192


def test_periodic_metadata_default_is_enabled() -> None:
    rsync_parser = _build_rsync_parser()
    sync_args = rsync_parser.parse_args(["src", "127.0.0.1:dst"])
    assert sync_args.feedback is None
    assert sync_args.periodic_metadata_interval_s == pytest.approx(10.0)
    assert sync_args.revisit_incomplete_passes == 2
    assert sync_args.revisit_max_rounds_per_pass == 8
    assert sync_args.primary_feedback_max_rounds == 64
    assert sync_args.primary_feedback_max_seconds == pytest.approx(8.0)
    assert sync_args.repair_queue_max_pending_requests == 1024
    assert sync_args.repair_worker_max_chunks_per_burst == 256
    assert sync_args.initial_pass_repair_max_chunks_per_burst == 16
    assert sync_args.repair_worker_poll_interval_s == pytest.approx(0.01)
    assert sync_args.open_loop_max_rounds == 10


def test_parser_feedback_flags_support_auto_and_overrides() -> None:
    rsync_parser = _build_rsync_parser()
    sync_auto = rsync_parser.parse_args(["src", "127.0.0.1:dst"])
    sync_on = rsync_parser.parse_args(["src", "127.0.0.1:dst", "--feedback"])
    sync_off = rsync_parser.parse_args(["src", "127.0.0.1:dst", "--no-feedback"])
    assert sync_auto.feedback is None
    assert sync_on.feedback is True
    assert sync_off.feedback is False


def test_parser_supports_ssyncd_alias_subcommand() -> None:
    parser = _build_parser()
    args = parser.parse_args(["ssyncd", "--bind-port", "9010"])
    assert args.command == "ssyncd"
    assert args.bind_port == 9010


def test_parser_supports_monitor_subcommand() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        ["monitor", "--output-dir", "./received", "--refresh-interval-s", "0.2"]
    )
    assert args.command == "monitor"
    assert args.output_dir == Path("./received")
    assert args.refresh_interval_s == pytest.approx(0.2)
    assert args.monitor_ipc_socket is None
    assert args.log_level == "WARNING"


def test_parser_supports_forward_stream_quiet_s_flag() -> None:
    parser = _build_parser()
    ssyncd_default = parser.parse_args(["ssyncd"])
    assert ssyncd_default.forward_stream_quiet_s == pytest.approx(0.5)
    ssyncd_custom = parser.parse_args(["ssyncd", "--forward-stream-quiet-s", "0.1"])
    assert ssyncd_custom.forward_stream_quiet_s == pytest.approx(0.1)


def test_ssyncd_parser_accepts_forward_stream_quiet_s() -> None:
    parser = _build_ssyncd_parser()
    args = parser.parse_args(["--forward-stream-quiet-s", "2.0"])
    assert args.forward_stream_quiet_s == pytest.approx(2.0)


def test_ssyncd_parser_accepts_server_args() -> None:
    parser = _build_ssyncd_parser()
    args = parser.parse_args(["--bind-port", "9011", "--root-dir", "./rx"])
    assert args.bind_port == 9011
    assert args.root_dir == Path("./rx")
    assert args.monitor_ipc_socket is None


def test_top_level_rsync_parser_supports_options() -> None:
    parser = _build_rsync_parser()
    args = parser.parse_args(
        ["-r", "-n", "--skip-unchanged", "--include", "*.txt", "src", "127.0.0.1:dst"]
    )
    assert args.recursive is True
    assert args.dry_run is True
    assert args.skip_unchanged is True
    assert args.include == ["*.txt"]
    assert args.paths == ["src", "127.0.0.1:dst"]


@pytest.mark.parametrize(
    "argv",
    [
        ["src", "127.0.0.1:dst", "--chunk-size", "0"],
        ["src", "127.0.0.1:dst", "--dest-port", "0"],
        ["src", "127.0.0.1:dst", "--drop-rate", "1.5"],
        ["src", "127.0.0.1:dst", "--feedback-wait-s", "-1"],
        ["src", "127.0.0.1:dst", "--feedback-wait-s", "nan"],
        ["src", "127.0.0.1:dst", "--inter-packet-delay-s", "inf"],
    ],
)
def test_sync_parser_rejects_invalid_numeric_values(argv: list[str]) -> None:
    parser = _build_rsync_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(argv)


@pytest.mark.parametrize("value", ["nan", "inf"])
def test_ssyncd_parser_rejects_non_finite_forward_stream_quiet_s(value: str) -> None:
    parser = _build_ssyncd_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--forward-stream-quiet-s", value])


def test_sync_parser_verbose_from_config_increments_with_cli_flag() -> None:
    parser = _build_rsync_parser({"verbose": 2})
    args = parser.parse_args(["-v", "src", "127.0.0.1:dst"])
    assert args.verbose == 3


def test_main_routes_top_level_to_sync(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _fake_run_sync(args: Namespace) -> int:
        assert args.paths == ["src", "127.0.0.1:dst"]
        return 0

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    monkeypatch.setattr(cli_module, "_run_sync", _fake_run_sync)
    assert cli_module.main(["src", "127.0.0.1:dst"]) == 0


def test_main_rejects_sync_subcommand(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    exit_code = cli_module.main(["sync", "src", "127.0.0.1:dst"])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "deprecated" in captured.out


def test_main_routes_ssyncd_to_server(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _fake_run_server(args: Namespace) -> int:
        assert args.bind_port == 9012
        return 0

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    monkeypatch.setattr(cli_module, "_run_server", _fake_run_server)
    assert cli_module.main(["ssyncd", "--bind-port", "9012"]) == 0


def test_ssyncd_main_routes_to_server(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _fake_run_server(args: Namespace) -> int:
        assert args.bind_port == 9013
        return 0

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    monkeypatch.setattr(cli_module, "_run_server", _fake_run_server)
    assert cli_module.ssyncd_main(["--bind-port", "9013"]) == 0


def test_checksum_requires_skip_unchanged(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    parser = _build_rsync_parser()
    source_file = tmp_path / "data.bin"
    source_file.write_bytes(b"abc")
    args = parser.parse_args([str(source_file), "127.0.0.1:dst.bin", "--checksum"])
    exit_code = cli_module._run_sync(args)
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "--checksum requires --skip-unchanged" in captured.out


def test_directory_requires_recursive(tmp_path: Path) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(ValueError):
        _collect_sync_items(
            source_dir,
            "missions/pass-001/",
            recursive=False,
            includes=[],
            excludes=[],
        )


def test_collect_sync_items_respects_include_exclude(tmp_path: Path) -> None:
    source_dir = tmp_path / "payload"
    (source_dir / "a").mkdir(parents=True, exist_ok=True)
    (source_dir / "root.txt").write_text("root")
    (source_dir / "root.bin").write_text("bin")
    (source_dir / "a" / "nested.txt").write_text("nested")
    items = _collect_sync_items(
        source_dir,
        "missions/pass-001/",
        recursive=True,
        includes=["*.txt"],
        excludes=["a/*"],
    )
    remote_names = {remote_name for _, remote_name in items}
    assert remote_names == {"missions/pass-001/root.txt"}


def test_is_unchanged_by_size_and_mtime(tmp_path: Path) -> None:
    source_file = tmp_path / "data.bin"
    source_file.write_bytes(b"abc")
    mtime_ns = source_file.stat().st_mtime_ns
    remote = RemoteFileInfo(
        path="dst/data.bin",
        exists=True,
        size=3,
        mtime_ns=mtime_ns,
        sha256=None,
    )
    assert _is_unchanged(source_file, remote, checksum=False) is True


def test_is_unchanged_by_checksum(tmp_path: Path) -> None:
    source_file = tmp_path / "data.bin"
    source_file.write_bytes(b"abc")
    remote = RemoteFileInfo(
        path="dst/data.bin",
        exists=True,
        size=3,
        mtime_ns=0,
        sha256=b"\xba\x78\x16\xbf\x8f\x01\xcf\xea\x41\x41\x40\xde\x5d\xae\x22\x23"
        b"\xb0\x03\x61\xa3\x96\x17\x7a\x9c\xb4\x10\xff\x61\xf2\x00\x15\xad",
    )
    assert _is_unchanged(source_file, remote, checksum=True) is True


def test_open_loop_state_round_trip(tmp_path: Path) -> None:
    state_file = tmp_path / "state.json"
    _save_open_loop_state(state_file, {"127.0.0.1:9000:a.bin": 2, "127.0.0.1:9000:b.bin": 1})
    loaded = _load_open_loop_state(state_file)
    assert loaded == {"127.0.0.1:9000:a.bin": 2, "127.0.0.1:9000:b.bin": 1}


def test_order_items_for_open_loop_prefers_lowest_retransmissions(tmp_path: Path) -> None:
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    c = tmp_path / "c.bin"
    a.write_bytes(b"a")
    b.write_bytes(b"b")
    c.write_bytes(b"c")
    items = [(a, "a.bin"), (b, "b.bin"), (c, "c.bin")]
    counts = {
        "127.0.0.1:9000:a.bin": 3,
        "127.0.0.1:9000:b.bin": 1,
        "127.0.0.1:9000:c.bin": 1,
    }
    ordered = _order_items_for_open_loop(
        items,
        destination_host="127.0.0.1",
        destination_port=9000,
        counts=counts,
    )
    assert [item[1] for item in ordered] == ["b.bin", "c.bin", "a.bin"]


def test_run_sync_open_loop_uses_persistent_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a")
    (source_dir / "b.bin").write_bytes(b"b")
    state_file = tmp_path / "state.json"
    _save_open_loop_state(
        state_file,
        {
            "127.0.0.1:9000:a.bin": 5,
            "127.0.0.1:9000:b.bin": 1,
        },
    )

    send_order: list[str] = []

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = True

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            **_kwargs: object,
        ) -> SimpleNamespace:
            send_order.append(remote_name or file_path.name)
            return SimpleNamespace(
                transfer_id_hex="id",
                total_chunks=1,
                repaired_chunks=0,
                repair_rounds=0,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called in default open-loop mode")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--no-feedback",
            "--open-loop-max-rounds",
            "1",
            "--state-file",
            str(state_file),
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert send_order[0] == "b.bin"
    assert send_order[1] == "a.bin"


def test_run_sync_supports_multiple_destinations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a")

    send_targets: list[str] = []

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = True

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            **_kwargs: object,
        ) -> SimpleNamespace:
            send_targets.append(f"{destination_host}:{remote_name}")
            return SimpleNamespace(
                transfer_id_hex="id",
                total_chunks=1,
                repaired_chunks=0,
                repair_rounds=0,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--destination",
            "127.0.0.2:./",
            "--open-loop-max-rounds",
            "1",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert sorted(send_targets) == ["127.0.0.1:a.bin", "127.0.0.2:a.bin"]


def test_run_sync_uses_prefetched_checksum_override_when_supported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / "a.bin"
    source_file.write_bytes(b"a" * (1024 * 1024))

    checksum_overrides: list[bytes | None] = []
    call_count = 0

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = True

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            local_sha256_override: bytes | None = None,
            **_kwargs: object,
        ) -> SimpleNamespace:
            nonlocal call_count
            call_count += 1
            checksum_overrides.append(local_sha256_override)
            if call_count == 1:
                # Allow prefetch worker to finish and populate cache.
                time.sleep(0.1)
            return SimpleNamespace(
                transfer_id_hex=f"{call_count:032x}",
                total_chunks=1,
                repaired_chunks=0,
                repair_rounds=0,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--destination",
            "127.0.0.2:./",
            "--open-loop-max-rounds",
            "1",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert call_count == 2
    assert checksum_overrides[1] is not None
    assert len(checksum_overrides[1] or b"") == 32


def test_run_sync_feedback_revisits_incomplete_transfers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a" * 1024)
    (source_dir / "b.bin").write_bytes(b"b" * 1024)

    calls: list[dict[str, object]] = []

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = True

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            transfer_id: bytes | None = None,
            send_initial_data: bool = True,
            max_repair_rounds_override: int | None = None,
            max_feedback_seconds_override: float | None = None,
            max_feedback_total_rounds_override: int | None = None,
        ) -> SimpleNamespace:
            calls.append(
                {
                    "remote_name": remote_name,
                    "transfer_id": transfer_id,
                    "send_initial_data": send_initial_data,
                    "max_repair_rounds_override": max_repair_rounds_override,
                    "max_feedback_seconds_override": max_feedback_seconds_override,
                    "max_feedback_total_rounds_override": max_feedback_total_rounds_override,
                }
            )
            if send_initial_data and remote_name == "a.bin":
                return SimpleNamespace(
                    transfer_id_hex="11" * 16,
                    total_chunks=10,
                    repaired_chunks=2,
                    repair_rounds=1,
                    completed=False,
                )
            return SimpleNamespace(
                transfer_id_hex=("11" * 16) if remote_name == "a.bin" else ("22" * 16),
                total_chunks=10,
                repaired_chunks=4,
                repair_rounds=2,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--revisit-incomplete-passes",
            "2",
            "--revisit-max-rounds-per-pass",
            "3",
            "--open-loop-max-rounds",
            "1",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    revisit_calls = [call for call in calls if call["send_initial_data"] is False]
    primary_calls = [call for call in calls if call["send_initial_data"] is True]
    assert primary_calls
    assert primary_calls[0]["max_feedback_seconds_override"] == pytest.approx(8.0)
    assert primary_calls[0]["max_feedback_total_rounds_override"] == 64
    assert len(revisit_calls) >= 1
    assert revisit_calls[0]["transfer_id"] == bytes.fromhex("11" * 16)
    assert revisit_calls[0]["max_repair_rounds_override"] == 3
    assert revisit_calls[0]["max_feedback_seconds_override"] == pytest.approx(0.0)
    assert revisit_calls[0]["max_feedback_total_rounds_override"] == 0


def test_run_sync_revisit_prioritizes_current_then_checks_others(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a" * 1024)
    (source_dir / "b.bin").write_bytes(b"b" * 1024)

    call_order: list[tuple[str, bool]] = []
    revisit_counts: dict[str, int] = {"a.bin": 0, "b.bin": 0}

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = True

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            transfer_id: bytes | None = None,
            send_initial_data: bool = True,
            **_kwargs: object,
        ) -> SimpleNamespace:
            assert remote_name is not None
            call_order.append((remote_name, send_initial_data))
            if send_initial_data:
                return SimpleNamespace(
                    transfer_id_hex=("aa" * 16) if remote_name == "a.bin" else ("bb" * 16),
                    total_chunks=10,
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=False,
                )
            revisit_counts[remote_name] += 1
            return SimpleNamespace(
                transfer_id_hex=("aa" * 16) if remote_name == "a.bin" else ("bb" * 16),
                total_chunks=10,
                repaired_chunks=0,
                repair_rounds=0,
                completed=revisit_counts[remote_name] >= 2,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--revisit-incomplete-passes",
            "2",
            "--revisit-max-rounds-per-pass",
            "3",
            "--open-loop-max-rounds",
            "1",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    # Sequence should include both primary files and revisit coverage for both.
    primary_calls = [entry for entry in call_order if entry[1] is True]
    revisit_calls = [entry for entry in call_order if entry[1] is False]
    assert len(primary_calls) == 2
    assert len(revisit_calls) >= 2
    first_primary_name = primary_calls[0][0]
    second_primary_name = primary_calls[1][0]
    assert first_primary_name != second_primary_name
    revisit_names = {name for name, send_initial in revisit_calls if not send_initial}
    assert first_primary_name in revisit_names
    assert second_primary_name in revisit_names


def test_run_sync_auto_feedback_transition_stops_open_loop_repeats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a" * 1024)

    send_calls = 0

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = False

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            **_kwargs: object,
        ) -> SimpleNamespace:
            nonlocal send_calls
            send_calls += 1
            if send_calls == 1:
                return SimpleNamespace(
                    transfer_id_hex="11" * 16,
                    total_chunks=10,
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=True,
                )
            self._auto_feedback_active = True
            return SimpleNamespace(
                transfer_id_hex="22" * 16,
                total_chunks=10,
                repaired_chunks=5,
                repair_rounds=2,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--open-loop-max-rounds",
            "0",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert send_calls == 2


def test_run_sync_auto_feedback_idle_demotion_resumes_open_loop_repeats(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a" * 1024)

    send_calls = 0

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = True
            self._last_auto_uplink_activity_s = time.monotonic() - 120.0

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            **_kwargs: object,
        ) -> SimpleNamespace:
            nonlocal send_calls
            send_calls += 1
            return SimpleNamespace(
                transfer_id_hex="11" * 16,
                total_chunks=10,
                repaired_chunks=0,
                repair_rounds=0,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--open-loop-max-rounds",
            "2",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert send_calls == 2


def test_run_sync_defers_revisits_until_feedback_becomes_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a" * 1024)

    primary_calls = 0
    revisit_calls = 0

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = False
            self._last_auto_uplink_activity_s: float | None = None

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            send_initial_data: bool = True,
            **_kwargs: object,
        ) -> SimpleNamespace:
            nonlocal primary_calls, revisit_calls
            if send_initial_data:
                primary_calls += 1
                if primary_calls == 1:
                    return SimpleNamespace(
                        transfer_id_hex="11" * 16,
                        total_chunks=10,
                        repaired_chunks=0,
                        repair_rounds=0,
                        completed=False,
                    )
                self._auto_feedback_active = True
                self._last_auto_uplink_activity_s = time.monotonic()
                return SimpleNamespace(
                    transfer_id_hex="22" * 16,
                    total_chunks=10,
                    repaired_chunks=2,
                    repair_rounds=1,
                    completed=True,
                )
            assert self._auto_feedback_active is True
            revisit_calls += 1
            return SimpleNamespace(
                transfer_id_hex="11" * 16,
                total_chunks=10,
                repaired_chunks=1,
                repair_rounds=1,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--open-loop-max-rounds",
            "2",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert primary_calls == 2
    assert revisit_calls >= 1


def test_run_sync_revisit_no_progress_does_not_consume_pass_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = tmp_path / "payload"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "a.bin").write_bytes(b"a" * 1024)
    (source_dir / "b.bin").write_bytes(b"b" * 1024)

    a_revisit_calls = 0

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config
            self._auto_feedback_active = True

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            send_initial_data: bool = True,
            **_kwargs: object,
        ) -> SimpleNamespace:
            nonlocal a_revisit_calls
            if send_initial_data:
                if remote_name == "a.bin":
                    return SimpleNamespace(
                        transfer_id_hex="11" * 16,
                        total_chunks=10,
                        repaired_chunks=0,
                        repair_rounds=0,
                        completed=False,
                    )
                return SimpleNamespace(
                    transfer_id_hex="22" * 16,
                    total_chunks=10,
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=True,
                )
            assert remote_name == "a.bin"
            a_revisit_calls += 1
            if a_revisit_calls == 1:
                # No STATUS-driven repair progress; should not burn revisit pass budget.
                return SimpleNamespace(
                    transfer_id_hex="11" * 16,
                    total_chunks=10,
                    repaired_chunks=0,
                    repair_rounds=0,
                    completed=False,
                )
            return SimpleNamespace(
                transfer_id_hex="11" * 16,
                total_chunks=10,
                repaired_chunks=1,
                repair_rounds=1,
                completed=True,
            )

        def query_remote_file(self, **kwargs: object) -> object:
            raise AssertionError("query_remote_file should not be called without --skip-unchanged")

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            "-r",
            str(source_dir),
            "127.0.0.1:./",
            "--revisit-incomplete-passes",
            "1",
            "--open-loop-max-rounds",
            "1",
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert a_revisit_calls >= 2


def test_run_sync_expands_source_wildcards(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "a.bin").write_bytes(b"a")
    (tmp_path / "b.bin").write_bytes(b"b")

    parser = _build_rsync_parser()
    args = parser.parse_args(
        [
            str(tmp_path / "*.bin"),
            "127.0.0.1:incoming/",
            "--dry-run",
        ]
    )
    exit_code = cli_module._run_sync(args)
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "would_send=2" in captured.out


def test_config_local_sync_chunk_size_applies_to_parser(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        '[sync]\nchunk_size = 7777\n',
        encoding="utf-8",
    )
    cfg = load_cli_config_defaults("sync")
    parser = _build_rsync_parser(cfg)
    args = parser.parse_args(["src", "127.0.0.1:dst"])
    assert args.chunk_size == 7777


def test_config_later_file_overrides_earlier(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    (home / ".config" / "ssync").mkdir(parents=True)
    (home / ".config" / "ssync" / "config.toml").write_text(
        "[sync]\nchunk_size = 100\n",
        encoding="utf-8",
    )
    (home / ".ssync.toml").write_text(
        "[sync]\nchunk_size = 200\n",
        encoding="utf-8",
    )
    (tmp_path / ".ssync.toml").write_text(
        "[sync]\nchunk_size = 300\n",
        encoding="utf-8",
    )
    cfg = load_cli_config_defaults("sync")
    parser = _build_rsync_parser(cfg)
    args = parser.parse_args(["src", "127.0.0.1:dst"])
    assert args.chunk_size == 300


def test_config_cli_overrides_toml(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        "[sync]\nchunk_size = 5000\n",
        encoding="utf-8",
    )
    cfg = load_cli_config_defaults("sync")
    parser = _build_rsync_parser(cfg)
    args = parser.parse_args(["src", "127.0.0.1:dst", "--chunk-size", "9999"])
    assert args.chunk_size == 9999


def test_config_unknown_key_in_section_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        "[sync]\nchunk_siz = 1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown config key"):
        load_cli_config_defaults("sync")


def test_config_sync_delete_key_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        "[sync]\ndelete = true\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown config key"):
        load_cli_config_defaults("sync")


def test_config_server_bind_port_allows_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        "[server]\nbind_port = 0\n",
        encoding="utf-8",
    )
    cfg = load_cli_config_defaults("server")
    parser = _build_ssyncd_parser(cfg)
    args = parser.parse_args([])
    assert args.bind_port == 0


def test_config_sync_verbose_boolean_is_supported(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        "[sync]\nverbose = true\n",
        encoding="utf-8",
    )
    cfg = load_cli_config_defaults("sync")
    parser = _build_rsync_parser(cfg)
    args = parser.parse_args(["src", "127.0.0.1:dst"])
    assert args.verbose == 1


@pytest.mark.parametrize(
    "toml",
    [
        "[sync]\ndrop_rate = nan\n",
        "[sync]\ndrop_rate = inf\n",
    ],
)
def test_config_drop_rate_rejects_non_finite(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, toml: str
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(toml, encoding="utf-8")
    with pytest.raises(ValueError, match="must be finite"):
        load_cli_config_defaults("sync")


def test_config_all_supported_keys_have_validator_classification() -> None:
    all_keys = (
        config_file_module._GLOBAL_KEYS
        | config_file_module._MONITOR_KEYS
        | config_file_module._SERVER_KEYS
        | config_file_module._SYNC_KEYS
    )
    classified = (
        config_file_module._GLOBAL_KEYS
        | config_file_module._APPEND_LIST_KEYS
        | config_file_module._PATH_KEYS
        | config_file_module._STRING_KEYS
        | config_file_module._BOOL_KEYS
        | config_file_module._BIND_PORT_KEYS
        | config_file_module._DEST_PORT_KEYS
        | config_file_module._POSITIVE_INT_KEYS
        | config_file_module._NONNEGATIVE_INT_KEYS
        | config_file_module._NONNEGATIVE_FLOAT_KEYS
        | frozenset({"drop_rate", "verbose"})
    )
    assert all_keys <= classified


def test_config_unknown_section_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        "[bogus]\nx = 1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Unknown config section"):
        load_cli_config_defaults("sync")


@pytest.mark.parametrize(
    ("toml", "match"),
    [
        ("[sync]\ndest_port = 70000\n", "must be <= 65535"),
        ('[sync]\nchunk_size = "large"\n', "expected integer"),
        ("[sync]\nchunk_size = 0\n", "must be >= 1"),
        ("[sync]\nfeedback_wait_s = -1.0\n", "must be >= 0"),
        ('[server]\nfeedback = "yes"\n', "expected boolean"),
        ("[server]\nroot_dir = 123\n", "expected string"),
    ],
)
def test_config_invalid_types_and_ranges_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    toml: str,
    match: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(toml, encoding="utf-8")
    command = "server" if "[server]" in toml else "sync"
    with pytest.raises(ValueError, match=match):
        load_cli_config_defaults(command)


def test_config_non_utf8_file_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_bytes(b"\xff")
    with pytest.raises(ValueError, match="Invalid UTF-8"):
        load_cli_config_defaults("sync")


def test_config_sync_lists_from_toml(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        '[sync]\ndestinations = ["127.0.0.2:extra/"]\n'
        'include = ["*.bin"]\n'
        'exclude = ["*.tmp"]\n',
        encoding="utf-8",
    )
    cfg = load_cli_config_defaults("sync")
    parser = _build_rsync_parser(cfg)
    args = parser.parse_args(["src", "127.0.0.1:dst"])
    assert args.destinations == ["127.0.0.2:extra/"]
    assert args.include == ["*.bin"]
    assert args.exclude == ["*.tmp"]


def test_config_sync_list_cli_values_replace_toml(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        '[sync]\ndestinations = ["127.0.0.2:extra/"]\n'
        'include = ["*.bin"]\n'
        'exclude = ["*.tmp"]\n',
        encoding="utf-8",
    )
    cfg = load_cli_config_defaults("sync")
    parser = _build_rsync_parser(cfg)
    args = parser.parse_args(
        [
            "--destination",
            "127.0.0.3:cli/",
            "--include",
            "*.txt",
            "--exclude",
            "*.bak",
            "src",
            "127.0.0.1:dst",
        ]
    )
    assert args.destinations == ["127.0.0.3:cli/"]
    assert args.include == ["*.txt"]
    assert args.exclude == ["*.bak"]


def test_main_config_load_error_returns_2(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    (tmp_path / "home").mkdir()
    (tmp_path / ".ssync.toml").write_text(
        "[sync]\nchunk_siz = 1\n",
        encoding="utf-8",
    )
    exit_code = cli_module.main(["src", "127.0.0.1:dst"])
    assert exit_code == 2
    assert "config error" in capsys.readouterr().err
