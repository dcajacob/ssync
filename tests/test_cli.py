from __future__ import annotations

from pathlib import Path

import pytest

from ssync.space_sync.cli import _collect_sync_items, _parse_destination


def test_parse_destination_valid() -> None:
    host, remote = _parse_destination("127.0.0.1:dropzone/file.bin")
    assert host == "127.0.0.1"
    assert remote == "dropzone/file.bin"


@pytest.mark.parametrize(
    "value",
    [
        "127.0.0.1",
        ":dropzone/file.bin",
        "127.0.0.1:",
        "127.0.0.1:/absolute/path",
    ],
)
def test_parse_destination_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        _parse_destination(value)


def test_collect_sync_items_file_explicit_name(tmp_path: Path) -> None:
    source_file = tmp_path / "data.bin"
    source_file.write_bytes(b"abc")
    items = _collect_sync_items(source_file, "incoming/final.bin")
    assert items == [(source_file.resolve(), "incoming/final.bin")]


def test_collect_sync_items_file_directory_target(tmp_path: Path) -> None:
    source_file = tmp_path / "data.bin"
    source_file.write_bytes(b"abc")
    items = _collect_sync_items(source_file, "incoming/")
    assert items == [(source_file.resolve(), "incoming/data.bin")]


def test_collect_sync_items_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "payload"
    (source_dir / "a").mkdir(parents=True, exist_ok=True)
    (source_dir / "root.txt").write_text("root")
    (source_dir / "a" / "nested.txt").write_text("nested")
    items = _collect_sync_items(source_dir, "missions/pass-001/")
    remote_names = {remote_name for _, remote_name in items}
    assert remote_names == {
        "missions/pass-001/root.txt",
        "missions/pass-001/a/nested.txt",
    }
