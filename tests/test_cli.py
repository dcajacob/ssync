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
    send_args = parser.parse_args(["send", "data.bin", "--json", "--beacon-interval-s", "2.5"])
    sync_args = parser.parse_args(
        ["sync", "src", "127.0.0.1:dst", "--json", "--beacon-interval-s", "0"]
    )
    assert send_args.json_output is True
    assert sync_args.json_output is True
    assert send_args.files == ["data.bin"]
    assert send_args.beacon_interval_s == pytest.approx(2.5)
    assert sync_args.beacon_interval_s == pytest.approx(0.0)


def test_parser_supports_ssyncd_alias_subcommand() -> None:
    parser = _build_parser()
    args = parser.parse_args(["ssyncd", "--bind-port", "9010"])
    assert args.command == "ssyncd"
    assert args.bind_port == 9010


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
        ]
    )
    exit_code = cli_module._run_sync(args)
    assert exit_code == 0
    assert sorted(send_targets) == ["127.0.0.1:a.bin", "127.0.0.2:a.bin"]


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
