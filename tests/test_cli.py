from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

from ssync.space_sync import cli as cli_module
from ssync.space_sync.cli import (
    _build_parser,
    _build_rsync_parser,
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
    send_args = parser.parse_args(["send", "data.bin", "--json"])
    sync_args = parser.parse_args(["sync", "src", "127.0.0.1:dst", "--json"])
    assert send_args.json_output is True
    assert sync_args.json_output is True


def test_top_level_rsync_parser_supports_options() -> None:
    parser = _build_rsync_parser()
    args = parser.parse_args(
        ["-r", "-n", "--skip-unchanged", "--include", "*.txt", "src", "127.0.0.1:dst"]
    )
    assert args.recursive is True
    assert args.dry_run is True
    assert args.skip_unchanged is True
    assert args.include == ["*.txt"]


def test_main_routes_top_level_to_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_run_sync(args: Namespace) -> int:
        assert args.source.name == "src"
        assert args.destination == "127.0.0.1:dst"
        return 0

    monkeypatch.setattr(cli_module, "_run_sync", _fake_run_sync)
    assert cli_module.main(["src", "127.0.0.1:dst"]) == 0


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
