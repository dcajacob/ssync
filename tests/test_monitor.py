from __future__ import annotations

import json
from pathlib import Path

from rich.console import Console

from ssync.space_sync.monitor import (
    _CLEAR_CONFIRM_KEY,
    _FILE_COLUMN_WIDTH,
    TransferSnapshot,
    _autoselect_active_transfer_index,
    _autoselect_new_transfer_index,
    _build_hole_map,
    _build_hole_map_2d,
    _estimate_overall_mode,
    _estimate_transfer_mode,
    _format_transfer_file_name,
    _merge_monitor_ipc_events,
    _read_transfer_snapshots,
    _render_monitor,
    _stabilize_overall_mode,
    _visible_transfer_window,
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


def test_reset_baseline_prevents_pre_reset_repairs_from_resurrecting() -> None:
    """After pressing 'r', pre-reset repairs should not reappear when an
    active transfer completes and retires into cumulative_repairs."""
    transfer_a = TransferSnapshot(
        transfer_id_hex="aaa",
        file_name="a.bin",
        file_size=1024,
        total_chunks=16,
        chunk_size=64,
        received_chunks=12,
        range_count=1,
        received_ranges=[(0, 12)],
        stream_cursor_chunk=11,
        backfill_chunks=5,
    )
    transfer_b = TransferSnapshot(
        transfer_id_hex="bbb",
        file_name="b.bin",
        file_size=2048,
        total_chunks=32,
        chunk_size=64,
        received_chunks=20,
        range_count=1,
        received_ranges=[(0, 20)],
        stream_cursor_chunk=19,
        backfill_chunks=3,
    )
    snapshots = [transfer_a, transfer_b]

    last_backfill_by_id: dict[str, int] = {
        "aaa": 5,
        "bbb": 3,
    }
    cumulative_repairs = 0

    backfill_baseline_by_id = {
        s.transfer_id_hex: s.backfill_chunks for s in snapshots
    }
    last_backfill_by_id = dict(backfill_baseline_by_id)

    assert last_backfill_by_id == {"aaa": 5, "bbb": 3}
    assert backfill_baseline_by_id == {"aaa": 5, "bbb": 3}

    last_backfill_by_id["aaa"] = 7

    stale_ids = {"aaa"}
    for stale_id in stale_ids:
        raw = last_backfill_by_id.pop(stale_id, 0)
        baseline = backfill_baseline_by_id.pop(stale_id, 0)
        cumulative_repairs += max(0, raw - baseline)

    assert cumulative_repairs == 2
    assert "aaa" not in last_backfill_by_id
    assert "aaa" not in backfill_baseline_by_id

    active_backfill = sum(
        max(0, s.backfill_chunks - backfill_baseline_by_id.get(s.transfer_id_hex, 0))
        for s in [transfer_b]
    )
    assert active_backfill == 0


def test_reset_preserves_last_backfill_for_immediate_retirement() -> None:
    """If a transfer disappears right after reset, its post-reset repairs
    should still be tracked correctly (raw - baseline = 0, not negative)."""
    backfill_baseline_by_id = {"x": 10}
    last_backfill_by_id = dict(backfill_baseline_by_id)
    cumulative_repairs = 0

    raw = last_backfill_by_id.pop("x", 0)
    baseline = backfill_baseline_by_id.pop("x", 0)
    cumulative_repairs += max(0, raw - baseline)

    assert cumulative_repairs == 0


def test_visible_transfer_window_follows_selection() -> None:
    assert _visible_transfer_window(0, selected_index=0) == (0, 0)
    assert _visible_transfer_window(3, selected_index=1) == (0, 3)
    assert _visible_transfer_window(8, selected_index=0) == (0, 5)
    assert _visible_transfer_window(8, selected_index=4) == (0, 5)
    assert _visible_transfer_window(8, selected_index=5) == (1, 6)
    assert _visible_transfer_window(8, selected_index=7) == (3, 8)


def test_render_monitor_limits_active_transfer_rows_to_five() -> None:
    snapshots = [
        TransferSnapshot(
            transfer_id_hex=f"id{i:02d}",
            file_name=f"file-{i}.bin",
            file_size=1024,
            total_chunks=16,
            chunk_size=64,
            received_chunks=min(i + 1, 16),
            range_count=1,
            received_ranges=[(0, min(i + 1, 16))],
            stream_cursor_chunk=min(i, 15),
        )
        for i in range(7)
    ]
    console = Console(record=True, width=140)
    console.print(
        _render_monitor(
            output_dir=Path("/tmp/rx"),
            snapshots=snapshots,
            throughput_bps={},
            selected_index=6,
            completed_count=0,
            completed_size=0,
        )
    )
    rendered = console.export_text()

    assert "file-0.bin" not in rendered
    assert "file-1.bin" not in rendered
    assert "file-2.bin" in rendered
    assert "file-3.bin" in rendered
    assert "file-4.bin" in rendered
    assert "file-5.bin" in rendered
    assert "file-6.bin" in rendered
    assert "(3-7/7)" in rendered


def test_format_transfer_file_name_scrolls_only_when_highlighted() -> None:
    long_name = "abcdefghijklmnopqrstuvwxyz0123456789-extra-long.bin"

    plain = _format_transfer_file_name(
        long_name,
        highlighted=False,
        now_s=0.0,
    )
    assert plain.plain == long_name

    scrolled_a = _format_transfer_file_name(
        long_name,
        highlighted=True,
        now_s=0.0,
    )
    scrolled_b = _format_transfer_file_name(
        long_name,
        highlighted=True,
        now_s=1.0,
    )

    assert len(scrolled_a.plain) == _FILE_COLUMN_WIDTH
    assert len(scrolled_b.plain) == _FILE_COLUMN_WIDTH
    assert scrolled_a.plain != scrolled_b.plain
    assert "\n" not in scrolled_a.plain
    assert "\n" not in scrolled_b.plain


def test_render_monitor_shows_clear_help_and_status_message() -> None:
    console = Console(record=True, width=140)
    console.print(
        _render_monitor(
            output_dir=Path("/tmp/rx"),
            snapshots=[],
            throughput_bps={},
            selected_index=0,
            completed_count=0,
            completed_size=0,
            status_message=("Press x again to clear", "yellow"),
        )
    )
    rendered = console.export_text()
    assert f"{_CLEAR_CONFIRM_KEY}=clear received dir" in rendered
    assert "Press x again to clear" in rendered



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
