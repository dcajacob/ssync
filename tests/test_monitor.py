from __future__ import annotations

import json
from pathlib import Path

from ssync.space_sync.monitor import (
    TransferSnapshot,
    _autoselect_active_transfer_index,
    _autoselect_new_transfer_index,
    _build_hole_map,
    _build_hole_map_2d,
    _estimate_overall_mode,
    _estimate_transfer_mode,
    _merge_monitor_ipc_events,
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
    assert snapshot.last_beacon_tx_s == 0.0
    assert snapshot.last_beacon_rx_s == 0.0


def test_read_transfer_snapshots_parses_beacon_timestamps(tmp_path: Path) -> None:
    output_dir = tmp_path / "rx"
    output_dir.mkdir(parents=True, exist_ok=True)
    journal_path = output_dir / ".ssync-journal.json"
    journal_path.write_text(
        json.dumps(
            {
                "transfers": [
                    {
                        "transfer_id_hex": "beacon123",
                        "manifest": {
                            "file_name": "beacon.bin",
                            "file_size": 1024,
                            "chunk_size": 128,
                            "total_chunks": 8,
                        },
                        "received_ranges": [[0, 2]],
                        "highest_chunk_seen": 1,
                        "last_chunk_seen": 1,
                        "last_beacon_tx_s": 12.5,
                        "last_beacon_rx_s": 13.25,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    snapshots = _read_transfer_snapshots(output_dir)
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.transfer_id_hex == "beacon123"
    assert snapshot.last_beacon_tx_s == 12.5
    assert snapshot.last_beacon_rx_s == 13.25


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


def test_estimate_overall_mode_promotes_if_any_transfer_has_backfill() -> None:
    backfill_snapshot = TransferSnapshot(
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
    forward_only_snapshot = TransferSnapshot(
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
    label, _style = _estimate_overall_mode([forward_only_snapshot, backfill_snapshot])
    assert label == "FEEDBACK"


def test_estimate_overall_mode_detects_feedback_from_backfill() -> None:
    snapshot = TransferSnapshot(
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
    label, _style = _estimate_overall_mode([snapshot])
    assert label == "FEEDBACK"


def test_estimate_overall_mode_no_feedback_without_backfill() -> None:
    snapshot = TransferSnapshot(
        transfer_id_hex="id-no-feedback",
        file_name="f.bin",
        file_size=2048,
        total_chunks=100,
        chunk_size=64,
        received_chunks=20,
        range_count=1,
        received_ranges=[(0, 20)],
        stream_cursor_chunk=20,
    )
    label, _style = _estimate_overall_mode([snapshot])
    assert label == "NO-FEEDBACK"


def test_stabilize_overall_mode_holds_feedback_briefly_after_loss() -> None:
    displayed, since_s, last_feedback_s = _stabilize_overall_mode(
        displayed_mode=("NO-FEEDBACK", "yellow"),
        candidate_mode=("FEEDBACK", "magenta"),
        now_s=100.0,
        displayed_since_s=95.0,
        last_feedback_seen_s=float("-inf"),
    )
    assert displayed[0] == "FEEDBACK"


def test_autoselect_active_transfer_prefers_recent_activity() -> None:
    snapshots = [
        TransferSnapshot(
            transfer_id_hex="idle",
            file_name="idle.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=8,
            range_count=1,
            received_ranges=[(0, 8)],
            stream_cursor_chunk=7,
        ),
        TransferSnapshot(
            transfer_id_hex="active",
            file_name="active.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=10,
            range_count=2,
            received_ranges=[(0, 8), (9, 11)],
            stream_cursor_chunk=10,
        ),
    ]
    selected = _autoselect_active_transfer_index(
        snapshots,
        selected_index=0,
        activity_bytes={"active": 256},
        throughput_bps={"idle": 1000.0, "active": 500.0},
    )
    assert selected == 1


def test_autoselect_active_transfer_falls_back_to_throughput() -> None:
    snapshots = [
        TransferSnapshot(
            transfer_id_hex="slow",
            file_name="slow.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=12,
            range_count=1,
            received_ranges=[(0, 12)],
            stream_cursor_chunk=11,
        ),
        TransferSnapshot(
            transfer_id_hex="fast",
            file_name="fast.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=9,
            range_count=1,
            received_ranges=[(0, 9)],
            stream_cursor_chunk=8,
        ),
    ]
    selected = _autoselect_active_transfer_index(
        snapshots,
        selected_index=0,
        activity_bytes={},
        throughput_bps={"slow": 1000.0, "fast": 2000.0},
    )
    assert selected == 1


def test_autoselect_new_transfer_only_considers_new_ids() -> None:
    snapshots = [
        TransferSnapshot(
            transfer_id_hex="existing",
            file_name="existing.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=15,
            range_count=1,
            received_ranges=[(0, 15)],
            stream_cursor_chunk=14,
        ),
        TransferSnapshot(
            transfer_id_hex="new",
            file_name="new.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=1,
            range_count=1,
            received_ranges=[(0, 1)],
            stream_cursor_chunk=0,
        ),
    ]
    selected = _autoselect_new_transfer_index(
        snapshots,
        selected_index=0,
        new_transfer_ids={"new"},
        activity_bytes={"existing": 1024, "new": 64},
        throughput_bps={"existing": 1_000_000.0, "new": 10_000.0},
    )
    assert selected == 1


def test_autoselect_new_transfer_keeps_manual_selection_without_new_ids() -> None:
    snapshots = [
        TransferSnapshot(
            transfer_id_hex="a",
            file_name="a.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=8,
            range_count=1,
            received_ranges=[(0, 8)],
            stream_cursor_chunk=7,
        ),
        TransferSnapshot(
            transfer_id_hex="b",
            file_name="b.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=9,
            range_count=1,
            received_ranges=[(0, 9)],
            stream_cursor_chunk=8,
        ),
    ]
    selected = _autoselect_new_transfer_index(
        snapshots,
        selected_index=1,
        new_transfer_ids=set(),
        activity_bytes={"a": 1000, "b": 0},
        throughput_bps={"a": 2000.0, "b": 1000.0},
    )
    assert selected == 1


def test_merge_monitor_ipc_beacon_event_updates_snapshot() -> None:
    snapshot = TransferSnapshot(
        transfer_id_hex="abc123",
        file_name="demo.bin",
        file_size=1024,
        total_chunks=8,
        chunk_size=128,
        received_chunks=2,
        range_count=1,
        received_ranges=[(0, 2)],
        stream_cursor_chunk=1,
        last_beacon_tx_s=0.0,
        last_beacon_rx_s=0.0,
    )
    merged = _merge_monitor_ipc_events(
        [snapshot],
        [{"type": "beacon_rx", "transfer_id_hex": "abc123", "ts_s": 42.0}],
    )
    assert len(merged) == 1
    assert merged[0].last_beacon_rx_s == 42.0


def test_merge_monitor_ipc_terminal_event_removes_snapshot() -> None:
    snapshot = TransferSnapshot(
        transfer_id_hex="abc123",
        file_name="demo.bin",
        file_size=1024,
        total_chunks=8,
        chunk_size=128,
        received_chunks=2,
        range_count=1,
        received_ranges=[(0, 2)],
        stream_cursor_chunk=1,
    )
    merged = _merge_monitor_ipc_events(
        [snapshot],
        [{"type": "transfer_terminal", "transfer_id_hex": "abc123"}],
    )
    assert merged == []
