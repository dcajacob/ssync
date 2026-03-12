from __future__ import annotations

import json
from pathlib import Path

from ssync.space_sync.monitor import (
    TransferSnapshot,
    _build_hole_map,
    _build_hole_map_2d,
    _estimate_overall_mode,
    _estimate_transfer_mode,
    _read_transfer_snapshots,
    _stabilize_overall_mode,
)


def test_read_transfer_snapshots_parses_ranges(tmp_path: Path) -> None:
    output_dir = tmp_path / "rx"
    output_dir.mkdir(parents=True, exist_ok=True)
    journal_path = output_dir / ".ssync-journal.json"
    journal_path.write_text(
        json.dumps(
            {
                "transfers": [
                    {
                        "transfer_id_hex": "abc123",
                        "manifest": {
                            "file_name": "demo.bin",
                            "file_size": 1024,
                            "chunk_size": 128,
                            "total_chunks": 8,
                        },
                        "received_ranges": [[0, 2], [4, 6]],
                        "highest_chunk_seen": 3,
                        "last_chunk_seen": 1,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    snapshots = _read_transfer_snapshots(output_dir)
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.transfer_id_hex == "abc123"
    assert snapshot.received_chunks == 4
    assert snapshot.range_count == 2
    assert snapshot.received_ranges == [(0, 2), (4, 6)]
    assert snapshot.stream_cursor_chunk == 1


def test_build_hole_map_shows_filled_and_holes() -> None:
    snapshot = TransferSnapshot(
        transfer_id_hex="id",
        file_name="f.bin",
        file_size=1024,
        total_chunks=16,
        chunk_size=64,
        received_chunks=8,
        range_count=1,
        received_ranges=[(0, 8)],
        stream_cursor_chunk=8,
    )
    hole_map = _build_hole_map(snapshot, width=16)
    plain = hole_map.plain
    assert "█" in plain
    assert "·" in plain


def test_build_hole_map_2d_shows_cursor() -> None:
    snapshot = TransferSnapshot(
        transfer_id_hex="id",
        file_name="f.bin",
        file_size=2048,
        total_chunks=32,
        chunk_size=64,
        received_chunks=16,
        range_count=1,
        received_ranges=[(0, 16)],
        stream_cursor_chunk=16,
    )
    hole_map = _build_hole_map_2d(snapshot, width=8, height=4)
    assert "▣" in hole_map.plain


def test_estimate_transfer_mode_detects_feedback_backfill() -> None:
    snapshot = TransferSnapshot(
        transfer_id_hex="id",
        file_name="f.bin",
        file_size=2048,
        total_chunks=100,
        chunk_size=64,
        received_chunks=40,
        range_count=3,
        received_ranges=[(0, 10), (15, 30), (35, 50)],
        stream_cursor_chunk=20,
    )
    label, _style = _estimate_transfer_mode(snapshot)
    assert label == "FEEDBACK"


def test_estimate_overall_mode_prefers_feedback() -> None:
    feedback_snapshot = TransferSnapshot(
        transfer_id_hex="id-feedback",
        file_name="f.bin",
        file_size=2048,
        total_chunks=100,
        chunk_size=64,
        received_chunks=40,
        range_count=3,
        received_ranges=[(0, 10), (15, 30), (35, 50)],
        stream_cursor_chunk=20,
    )
    no_feedback_snapshot = TransferSnapshot(
        transfer_id_hex="id-open",
        file_name="g.bin",
        file_size=1024,
        total_chunks=32,
        chunk_size=32,
        received_chunks=8,
        range_count=1,
        received_ranges=[(0, 8)],
        stream_cursor_chunk=7,
    )
    label, _style = _estimate_overall_mode([no_feedback_snapshot, feedback_snapshot])
    assert label == "FEEDBACK"


def test_stabilize_overall_mode_holds_feedback_briefly_after_loss() -> None:
    displayed, since_s, last_feedback_s = _stabilize_overall_mode(
        displayed_mode=("NO-FEEDBACK", "yellow"),
        candidate_mode=("FEEDBACK", "magenta"),
        now_s=100.0,
        displayed_since_s=95.0,
        last_feedback_seen_s=float("-inf"),
    )
    assert displayed[0] == "FEEDBACK"
    displayed, since_s, last_feedback_s = _stabilize_overall_mode(
        displayed_mode=displayed,
        candidate_mode=("NO-FEEDBACK", "yellow"),
        now_s=104.0,
        displayed_since_s=since_s,
        last_feedback_seen_s=last_feedback_s,
    )
    assert displayed[0] == "FEEDBACK"
