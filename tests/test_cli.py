from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from ssync.space_sync import cli as cli_module
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


def test_send_and_sync_support_json_flag() -> None:
    parser = _build_parser()
    rsync_parser = _build_rsync_parser()
    send_args = parser.parse_args(["send", "data.bin", "--json", "--beacon-interval-s", "2.5"])
    sync_args = rsync_parser.parse_args(
        ["src", "127.0.0.1:dst", "--json", "--beacon-interval-s", "0"]
    )
    assert send_args.json_output is True
    assert sync_args.json_output is True
    assert send_args.files == ["data.bin"]
    assert send_args.beacon_interval_s == pytest.approx(2.5)
    assert sync_args.beacon_interval_s == pytest.approx(0.0)


def test_parser_supports_metadata_alias_flags() -> None:
    parser = _build_parser()
    send_args = parser.parse_args(
        [
            "send",
            "data.bin",
            "--metadata-repeats",
            "5",
            "--periodic-metadata-interval-s",
            "1.5",
            "--periodic-metadata-every-n-chunks",
            "100",
        ]
    )
    receive_args = parser.parse_args(
        [
            "receive",
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
    assert send_args.manifest_repeats == 5
    assert send_args.periodic_metadata_interval_s == pytest.approx(1.5)
    assert send_args.periodic_metadata_every_n_chunks == 100
    assert receive_args.pre_metadata_max_pending_bytes == 123456
    assert receive_args.pre_metadata_max_pending_bytes_per_transfer == 2048
    assert receive_args.pre_metadata_max_pending_transfers == 16
    assert receive_args.pre_metadata_ttl_s == pytest.approx(12.5)


def test_parser_supports_adaptive_leading_hole_repair_flags() -> None:
    parser = _build_parser()
    receive_args = parser.parse_args(
        [
            "receive",
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
    assert receive_args.adaptive_leading_hole_boost is True
    assert receive_args.leading_hole_start_threshold_chunks == 128
    assert receive_args.leading_hole_min_span_chunks == 4096
    assert receive_args.leading_hole_boost_multiplier == 6
    assert receive_args.leading_hole_max_repair_chunks_per_request == 8192


def test_periodic_metadata_default_is_enabled() -> None:
    parser = _build_parser()
    rsync_parser = _build_rsync_parser()
    send_args = parser.parse_args(["send", "data.bin"])
    sync_args = rsync_parser.parse_args(["src", "127.0.0.1:dst"])
    assert send_args.feedback is None
    assert sync_args.feedback is None
    assert send_args.periodic_metadata_interval_s == pytest.approx(10.0)
    assert sync_args.periodic_metadata_interval_s == pytest.approx(10.0)
    assert send_args.revisit_incomplete_passes == 2
    assert send_args.revisit_max_rounds_per_pass == 8
    assert send_args.primary_feedback_max_rounds == 0
    assert send_args.primary_feedback_max_seconds == pytest.approx(0.0)
    assert sync_args.revisit_incomplete_passes == 2
    assert sync_args.revisit_max_rounds_per_pass == 8
    assert sync_args.primary_feedback_max_rounds == 64
    assert sync_args.primary_feedback_max_seconds == pytest.approx(8.0)


def test_parser_feedback_flags_support_auto_and_overrides() -> None:
    parser = _build_parser()
    rsync_parser = _build_rsync_parser()
    send_auto = parser.parse_args(["send", "data.bin"])
    send_on = parser.parse_args(["send", "data.bin", "--feedback"])
    send_off = parser.parse_args(["send", "data.bin", "--no-feedback"])
    sync_auto = rsync_parser.parse_args(["src", "127.0.0.1:dst"])
    sync_on = rsync_parser.parse_args(["src", "127.0.0.1:dst", "--feedback"])
    sync_off = rsync_parser.parse_args(["src", "127.0.0.1:dst", "--no-feedback"])
    assert send_auto.feedback is None
    assert send_on.feedback is True
    assert send_off.feedback is False
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
    assert args.log_level == "WARNING"


def test_ssyncd_parser_accepts_server_args() -> None:
    parser = _build_ssyncd_parser()
    args = parser.parse_args(["--bind-port", "9011", "--root-dir", "./rx"])
    assert args.bind_port == 9011
    assert args.root_dir == Path("./rx")


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


def test_main_routes_top_level_to_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_run_sync(args: Namespace) -> int:
        assert args.paths == ["src", "127.0.0.1:dst"]
        return 0

    monkeypatch.setattr(cli_module, "_run_sync", _fake_run_sync)
    assert cli_module.main(["src", "127.0.0.1:dst"]) == 0


def test_main_rejects_sync_subcommand(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = cli_module.main(["sync", "src", "127.0.0.1:dst"])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert "deprecated" in captured.out


def test_main_routes_ssyncd_to_server(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_run_server(args: Namespace) -> int:
        assert args.bind_port == 9012
        return 0

    monkeypatch.setattr(cli_module, "_run_server", _fake_run_server)
    assert cli_module.main(["ssyncd", "--bind-port", "9012"]) == 0


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


def test_run_sender_expands_quoted_wildcard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "a.deb").write_bytes(b"a")
    (tmp_path / "b.deb").write_bytes(b"b")
    send_order: list[str] = []

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            **_kwargs: object,
        ) -> SimpleNamespace:
            send_order.append(file_path.name)
            return SimpleNamespace(
                transfer_id_hex=file_path.name,
                total_chunks=1,
                repaired_chunks=0,
                repair_rounds=0,
                completed=True,
            )

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_parser()
    args = parser.parse_args(
        [
            "send",
            str(tmp_path / "*.deb"),
            "--dest-host",
            "127.0.0.1",
            "--dest-port",
            "9000",
        ]
    )
    exit_code = cli_module._run_sender(args)
    assert exit_code == 0
    assert send_order == ["a.deb", "b.deb"]


def test_run_sender_accepts_multiple_expanded_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a = tmp_path / "a.deb"
    b = tmp_path / "b.deb"
    a.write_bytes(b"a")
    b.write_bytes(b"b")
    send_order: list[str] = []

    class FakeSender:
        def __init__(self, config: object) -> None:
            self.config = config

        def send_file(
            self,
            file_path: Path,
            destination_host: str,
            destination_port: int,
            remote_name: str | None = None,
            stop_requested: object | None = None,
            **_kwargs: object,
        ) -> SimpleNamespace:
            send_order.append(file_path.name)
            return SimpleNamespace(
                transfer_id_hex=file_path.name,
                total_chunks=1,
                repaired_chunks=0,
                repair_rounds=0,
                completed=True,
            )

    monkeypatch.setattr(cli_module, "SpaceSyncSender", FakeSender)
    parser = _build_parser()
    args = parser.parse_args(
        [
            "send",
            str(a),
            str(b),
            "--dest-host",
            "127.0.0.1",
            "--dest-port",
            "9000",
        ]
    )
    exit_code = cli_module._run_sender(args)
    assert exit_code == 0
    assert send_order == ["a.deb", "b.deb"]
