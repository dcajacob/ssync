from __future__ import annotations

import json
import math
import os
import select
import socket
import sys
import termios
import time
import tty
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

_TermiosAttrs = list[int | list[bytes | int]]
_RATE_WINDOW_S = 3.0
_DETAIL_MAP_WIDTH = 96
_DETAIL_MAP_HEIGHT = 18
_COMPLETED_SCAN_ACTIVE_INTERVAL_S = 5.0
_COMPLETED_SCAN_IDLE_INTERVAL_S = 15.0
_INPUT_POLL_INTERVAL_S = 0.03
_MODE_FEEDBACK_STICKY_S = 30.0
_BEACON_LINK_STATE_FEEDBACK_THRESHOLD_MS = 5000
_MODE_MIN_SWITCH_DWELL_S = 2.0
_BEACON_FLASH_WINDOW_S = 0.6
_IPC_MAX_EVENTS_PER_REFRESH = 512


@dataclass(slots=True)
class TransferSnapshot:
    transfer_id_hex: str
    file_name: str
    file_size: int
    total_chunks: int
    chunk_size: int
    received_chunks: int
    range_count: int
    received_ranges: list[tuple[int, int]]
    stream_cursor_chunk: int
    last_beacon_tx_s: float = 0.0
    last_beacon_rx_s: float = 0.0
    last_sender_peer_age_ms: int = 0xFFFFFFFF
    backfill_chunks: int = 0

    @property
    def progress_ratio(self) -> float:
        if self.total_chunks <= 0:
            return 0.0
        return min(1.0, self.received_chunks / float(self.total_chunks))


def _estimate_transfer_mode(snapshot: TransferSnapshot) -> tuple[str, str]:
    if snapshot.total_chunks <= 0:
        return ("EMPTY", "grey58")
    if snapshot.range_count <= 1:
        if snapshot.progress_ratio >= 0.999:
            return ("COMPLETE", "green")
        return ("NO-FEEDBACK", "yellow")
    frontier_chunks = max(0, snapshot.stream_cursor_chunk + 1)
    if snapshot.received_chunks > frontier_chunks:
        # Backfill behind the forward cursor generally implies repair flow activity.
        return ("FEEDBACK", "magenta")
    return ("NO-FEEDBACK", "yellow")


def _estimate_overall_mode(
    snapshots: list[TransferSnapshot],
) -> tuple[str, str]:
    if not snapshots:
        return ("IDLE", "grey58")
    now_s = time.monotonic()
    for snapshot in snapshots:
        if snapshot.last_beacon_rx_s <= 0:
            continue
        if (now_s - snapshot.last_beacon_rx_s) > _BEACON_FLASH_WINDOW_S * 5:
            continue
        if snapshot.last_sender_peer_age_ms < _BEACON_LINK_STATE_FEEDBACK_THRESHOLD_MS:
            return ("FEEDBACK", "magenta")
    if any(
        _estimate_transfer_mode(snapshot)[0] == "FEEDBACK"
        for snapshot in snapshots
    ):
        return ("FEEDBACK", "magenta")
    return ("NO-FEEDBACK", "yellow")


def _stabilize_overall_mode(
    *,
    displayed_mode: tuple[str, str],
    candidate_mode: tuple[str, str],
    now_s: float,
    displayed_since_s: float,
    last_feedback_seen_s: float,
) -> tuple[tuple[str, str], float, float]:
    candidate_label, _candidate_style = candidate_mode
    current_label, _current_style = displayed_mode
    if candidate_label == "FEEDBACK":
        return candidate_mode, now_s, now_s
    if now_s - last_feedback_seen_s < _MODE_FEEDBACK_STICKY_S:
        return ("FEEDBACK", "magenta"), displayed_since_s, last_feedback_seen_s
    if candidate_label == current_label:
        return displayed_mode, displayed_since_s, last_feedback_seen_s
    if now_s - displayed_since_s < _MODE_MIN_SWITCH_DWELL_S:
        return displayed_mode, displayed_since_s, last_feedback_seen_s
    return candidate_mode, now_s, last_feedback_seen_s


def _safe_int(value: object, default: int = 0) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _safe_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _read_transfer_snapshots(output_dir: Path) -> list[TransferSnapshot]:
    journal_path = output_dir / ".ssync-journal.json"
    if not journal_path.exists():
        return []
    try:
        raw = json.loads(journal_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    transfers = raw.get("transfers", [])
    if not isinstance(transfers, list):
        return []
    snapshots: list[TransferSnapshot] = []
    for transfer in transfers:
        if not isinstance(transfer, dict):
            continue
        manifest = transfer.get("manifest", {})
        if not isinstance(manifest, dict):
            continue
        received_ranges_raw = transfer.get("received_ranges", [])
        if not isinstance(received_ranges_raw, list):
            received_ranges_raw = []
        received_chunks = 0
        range_count = 0
        received_ranges: list[tuple[int, int]] = []
        highest_seen_end = 0
        for item in received_ranges_raw:
            if not isinstance(item, list) or len(item) != 2:
                continue
            start = _safe_int(item[0])
            end = _safe_int(item[1])
            if end <= start:
                continue
            received_chunks += end - start
            range_count += 1
            received_ranges.append((start, end))
            if end > highest_seen_end:
                highest_seen_end = end
        snapshots.append(
            TransferSnapshot(
                transfer_id_hex=str(transfer.get("transfer_id_hex", "")),
                file_name=str(manifest.get("file_name", "")),
                file_size=_safe_int(manifest.get("file_size", 0)),
                total_chunks=_safe_int(manifest.get("total_chunks", 0)),
                chunk_size=_safe_int(manifest.get("chunk_size", 0)),
                received_chunks=received_chunks,
                range_count=range_count,
                received_ranges=received_ranges,
                stream_cursor_chunk=max(
                    0,
                    min(
                        _safe_int(
                            transfer.get(
                                "last_chunk_seen",
                                transfer.get("highest_chunk_seen", highest_seen_end - 1),
                            )
                        ),
                        max(0, _safe_int(manifest.get("total_chunks", 0)) - 1),
                    ),
                ),
                last_beacon_tx_s=_safe_float(transfer.get("last_beacon_tx_s", 0.0)),
                last_beacon_rx_s=_safe_float(transfer.get("last_beacon_rx_s", 0.0)),
                last_sender_peer_age_ms=_safe_int(
                    transfer.get("last_sender_peer_age_ms", 0xFFFFFFFF)
                ),
                backfill_chunks=_safe_int(transfer.get("backfill_chunks", 0)),
            )
        )
    snapshots.sort(key=lambda item: (item.file_name, item.transfer_id_hex))
    return snapshots


def _drain_monitor_ipc_events(
    sock: socket.socket | None,
    *,
    max_events: int = _IPC_MAX_EVENTS_PER_REFRESH,
) -> list[dict[str, object]]:
    if sock is None:
        return []
    events: list[dict[str, object]] = []
    for _ in range(max(1, max_events)):
        try:
            payload = sock.recv(65535)
        except (BlockingIOError, TimeoutError):
            break
        except OSError:
            break
        if not payload:
            break
        try:
            event = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def _merge_monitor_ipc_events(
    snapshots: list[TransferSnapshot],
    events: list[dict[str, object]],
) -> list[TransferSnapshot]:
    if not events:
        return snapshots
    by_id = {snapshot.transfer_id_hex: snapshot for snapshot in snapshots}
    for event in events:
        event_type = str(event.get("type", ""))
        transfer_id_hex = str(event.get("transfer_id_hex", ""))
        if not transfer_id_hex:
            continue
        current = by_id.get(transfer_id_hex)
        if event_type == "transfer_terminal":
            by_id.pop(transfer_id_hex, None)
            continue
        if event_type in {"beacon_tx", "beacon_rx"}:
            if current is None:
                continue
            ts_s = _safe_float(event.get("ts_s", 0.0))
            if event_type == "beacon_tx":
                current.last_beacon_tx_s = max(current.last_beacon_tx_s, ts_s)
            else:
                current.last_beacon_rx_s = max(current.last_beacon_rx_s, ts_s)
            continue
        if event_type != "transfer_update":
            continue
        if current is None:
            current = TransferSnapshot(
                transfer_id_hex=transfer_id_hex,
                file_name=str(event.get("file_name", "")),
                file_size=_safe_int(event.get("file_size", 0)),
                total_chunks=_safe_int(event.get("total_chunks", 0)),
                chunk_size=_safe_int(event.get("chunk_size", 0)),
                received_chunks=_safe_int(event.get("received_chunks", 0)),
                range_count=_safe_int(event.get("range_count", 0)),
                received_ranges=[],
                stream_cursor_chunk=_safe_int(event.get("stream_cursor_chunk", 0)),
                last_beacon_tx_s=_safe_float(event.get("last_beacon_tx_s", 0.0)),
                last_beacon_rx_s=_safe_float(event.get("last_beacon_rx_s", 0.0)),
                last_sender_peer_age_ms=_safe_int(
                    event.get("last_sender_peer_age_ms", 0xFFFFFFFF)
                ),
                backfill_chunks=_safe_int(event.get("backfill_chunks", 0)),
            )
            by_id[transfer_id_hex] = current
            continue
        current.file_name = str(event.get("file_name", current.file_name))
        current.file_size = _safe_int(event.get("file_size", current.file_size))
        current.total_chunks = _safe_int(event.get("total_chunks", current.total_chunks))
        current.chunk_size = _safe_int(event.get("chunk_size", current.chunk_size))
        current.received_chunks = _safe_int(event.get("received_chunks", current.received_chunks))
        current.range_count = _safe_int(event.get("range_count", current.range_count))
        current.stream_cursor_chunk = _safe_int(
            event.get("stream_cursor_chunk", current.stream_cursor_chunk)
        )
        current.last_beacon_tx_s = _safe_float(
            event.get("last_beacon_tx_s", current.last_beacon_tx_s)
        )
        current.last_beacon_rx_s = _safe_float(
            event.get("last_beacon_rx_s", current.last_beacon_rx_s)
        )
        current.last_sender_peer_age_ms = _safe_int(
            event.get("last_sender_peer_age_ms", current.last_sender_peer_age_ms)
        )
        current.backfill_chunks = _safe_int(
            event.get("backfill_chunks", current.backfill_chunks)
        )
    merged = list(by_id.values())
    merged.sort(key=lambda item: (item.file_name, item.transfer_id_hex))
    return merged


def _count_completed_files(output_dir: Path) -> tuple[int, int]:
    count = 0
    total_size = 0
    pending_dirs = [output_dir]
    while pending_dirs:
        current_dir = pending_dirs.pop()
        try:
            entries = list(os.scandir(current_dir))
        except OSError:
            continue
        for entry in entries:
            if entry.name.startswith("."):
                continue
            try:
                if entry.is_dir(follow_symlinks=False):
                    pending_dirs.append(Path(entry.path))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                stat_result = entry.stat(follow_symlinks=False)
            except OSError:
                continue
            total_size += stat_result.st_size
            count += 1
    return count, total_size


def _format_bytes(size_bytes: int) -> str:
    if size_bytes >= 1024**3:
        return f"{size_bytes / (1024**3):.2f} GiB"
    if size_bytes >= 1024**2:
        return f"{size_bytes / (1024**2):.2f} MiB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KiB"
    return f"{size_bytes} B"


def _format_bps(rate_bps: float) -> str:
    if rate_bps >= 1_000_000_000:
        return f"{rate_bps / 1_000_000_000:.2f} Gbps"
    if rate_bps >= 1_000_000:
        return f"{rate_bps / 1_000_000:.2f} Mbps"
    if rate_bps >= 1_000:
        return f"{rate_bps / 1_000:.2f} Kbps"
    return f"{rate_bps:.0f} bps"


def _progress_bar(width: int, ratio: float) -> Text:
    width = max(8, width)
    clamped = max(0.0, min(1.0, ratio))
    filled = int(clamped * width)
    text = Text("[")
    text.append("█" * filled, style="green")
    text.append("░" * (width - filled), style="grey50")
    text.append("]")
    return text


def _build_hole_map(snapshot: TransferSnapshot, width: int = 64) -> Text:
    width = max(16, width)
    if snapshot.total_chunks <= 0:
        return Text("no data", style="grey58")
    bucket_size = max(1, math.ceil(snapshot.total_chunks / width))
    ranges = sorted(snapshot.received_ranges)
    text = Text()
    range_index = 0
    for bucket_idx in range(width):
        bucket_start = bucket_idx * bucket_size
        bucket_end = min(snapshot.total_chunks, bucket_start + bucket_size)
        if bucket_end <= bucket_start:
            text.append(" ")
            continue
        filled = 0
        while range_index < len(ranges) and ranges[range_index][1] <= bucket_start:
            range_index += 1
        idx = range_index
        while idx < len(ranges):
            start, end = ranges[idx]
            if start >= bucket_end:
                break
            overlap_start = max(start, bucket_start)
            overlap_end = min(end, bucket_end)
            if overlap_end > overlap_start:
                filled += overlap_end - overlap_start
            idx += 1
        ratio = filled / float(bucket_end - bucket_start)
        if ratio >= 0.999:
            text.append("█", style="green")
        elif ratio <= 0.001:
            text.append("·", style="red")
        else:
            text.append("▒", style="yellow")
    return text


def _build_hole_map_2d(snapshot: TransferSnapshot, width: int, height: int) -> Text:
    width = max(16, width)
    height = max(4, height)
    total_cells = width * height
    if snapshot.total_chunks <= 0:
        return Text("no data", style="grey58")
    bucket_size = max(1, math.ceil(snapshot.total_chunks / total_cells))
    cursor_chunk = min(snapshot.stream_cursor_chunk, max(0, snapshot.total_chunks - 1))
    cursor_cell = min(total_cells - 1, cursor_chunk // bucket_size)
    ranges = sorted(snapshot.received_ranges)
    text = Text()
    range_index = 0
    for cell_idx in range(total_cells):
        bucket_start = cell_idx * bucket_size
        bucket_end = min(snapshot.total_chunks, bucket_start + bucket_size)
        if bucket_end <= bucket_start:
            glyph = " "
            style = "grey58"
        else:
            filled = 0
            while range_index < len(ranges) and ranges[range_index][1] <= bucket_start:
                range_index += 1
            idx = range_index
            while idx < len(ranges):
                start, end = ranges[idx]
                if start >= bucket_end:
                    break
                overlap_start = max(start, bucket_start)
                overlap_end = min(end, bucket_end)
                if overlap_end > overlap_start:
                    filled += overlap_end - overlap_start
                idx += 1
            ratio = filled / float(bucket_end - bucket_start)
            if ratio >= 0.999:
                glyph = "█"
                style = "green"
            elif ratio <= 0.001:
                glyph = "·"
                style = "red"
            else:
                glyph = "▒"
                style = "yellow"
        if cell_idx == cursor_cell:
            text.append("▣", style="bold cyan")
        else:
            text.append(glyph, style=style)
        if (cell_idx + 1) % width == 0 and (cell_idx + 1) < total_cells:
            text.append("\n")
    return text


def _autoselect_active_transfer_index(
    snapshots: list[TransferSnapshot],
    *,
    selected_index: int,
    activity_bytes: dict[str, int],
    throughput_bps: dict[str, float],
) -> int:
    if not snapshots:
        return 0
    clamped_selected = max(0, min(selected_index, len(snapshots) - 1))
    best_activity_index = -1
    best_activity_key = (0, 0.0, 0)
    for index, snapshot in enumerate(snapshots):
        activity = max(0, activity_bytes.get(snapshot.transfer_id_hex, 0))
        throughput = max(0.0, throughput_bps.get(snapshot.transfer_id_hex, 0.0))
        tie_breaker = snapshot.received_chunks
        key = (activity, throughput, tie_breaker)
        if key > best_activity_key:
            best_activity_key = key
            best_activity_index = index
    if best_activity_index >= 0 and best_activity_key[0] > 0:
        return best_activity_index
    best_rate_index = -1
    best_rate_key = (0.0, 0)
    for index, snapshot in enumerate(snapshots):
        throughput = max(0.0, throughput_bps.get(snapshot.transfer_id_hex, 0.0))
        rate_key = (throughput, snapshot.received_chunks)
        if rate_key > best_rate_key:
            best_rate_key = rate_key
            best_rate_index = index
    if best_rate_index >= 0 and best_rate_key[0] > 0.0:
        return best_rate_index
    return clamped_selected


def _autoselect_new_transfer_index(
    snapshots: list[TransferSnapshot],
    *,
    selected_index: int,
    new_transfer_ids: set[str],
    activity_bytes: dict[str, int],
    throughput_bps: dict[str, float],
) -> int:
    if not snapshots:
        return 0
    clamped_selected = max(0, min(selected_index, len(snapshots) - 1))
    if not new_transfer_ids:
        return clamped_selected
    best_new_index = -1
    best_new_key = (0, 0.0, 0, "")
    for index, snapshot in enumerate(snapshots):
        if snapshot.transfer_id_hex not in new_transfer_ids:
            continue
        activity = max(0, activity_bytes.get(snapshot.transfer_id_hex, 0))
        throughput = max(0.0, throughput_bps.get(snapshot.transfer_id_hex, 0.0))
        key = (
            activity,
            throughput,
            snapshot.received_chunks,
            snapshot.transfer_id_hex,
        )
        if key > best_new_key:
            best_new_key = key
            best_new_index = index
    if best_new_index >= 0:
        return best_new_index
    return clamped_selected


def _render_monitor(
    *,
    output_dir: Path,
    snapshots: list[TransferSnapshot],
    throughput_bps: dict[str, float],
    selected_index: int,
    completed_count: int,
    completed_size: int,
    cumulative_repairs: int = 0,
    overall_mode: tuple[str, str] | None = None,
) -> Group:
    if overall_mode is None:
        overall_mode_label, overall_mode_style = _estimate_overall_mode(snapshots)
    else:
        overall_mode_label, overall_mode_style = overall_mode
    summary = Text()
    summary.append("active=", style="bold")
    summary.append(str(len(snapshots)), style="cyan")
    summary.append("  completed_files=", style="bold")
    summary.append(str(completed_count), style="cyan")
    summary.append("  completed_bytes=", style="bold")
    summary.append(_format_bytes(completed_size), style="cyan")
    active_backfill = sum(s.backfill_chunks for s in snapshots)
    total_backfill = cumulative_repairs + active_backfill
    if total_backfill > 0 or cumulative_repairs > 0:
        summary.append("  repairs=", style="bold")
        summary.append(str(total_backfill), style="red")
        if cumulative_repairs > 0 and active_backfill > 0:
            summary.append(f" (file={active_backfill} prev={cumulative_repairs})", style="grey70")
    summary.append("  mode=", style="bold")
    summary.append(overall_mode_label, style=overall_mode_style)
    now_s = time.monotonic()
    beacon_rx_active = any(
        snapshot.last_beacon_rx_s > 0
        and (now_s - snapshot.last_beacon_rx_s) <= _BEACON_FLASH_WINDOW_S
        for snapshot in snapshots
    )
    beacon_tx_active = any(
        snapshot.last_beacon_tx_s > 0
        and (now_s - snapshot.last_beacon_tx_s) <= _BEACON_FLASH_WINDOW_S
        for snapshot in snapshots
    )
    beacon_summary = Text("beacons: ", style="grey62")
    beacon_summary.append("S->R ", style="grey62")
    beacon_summary.append("●", style="bold green" if beacon_rx_active else "grey35")
    beacon_summary.append("   ", style="grey62")
    beacon_summary.append("R->S ", style="grey62")
    beacon_summary.append("●", style="bold cyan" if beacon_tx_active else "grey35")

    table = Table(expand=True)
    table.add_column("File", overflow="fold")
    table.add_column("ID", width=10, no_wrap=True)
    table.add_column("Progress", width=22)
    table.add_column("Chunks", justify="right", no_wrap=True)
    table.add_column("Rate", justify="right", no_wrap=True)
    table.add_column("Ranges", justify="right", no_wrap=True)

    for index, snapshot in enumerate(snapshots):
        percent = snapshot.progress_ratio * 100.0
        rate_bps = throughput_bps.get(snapshot.transfer_id_hex, 0.0)
        table.add_row(
            snapshot.file_name or "<unknown>",
            snapshot.transfer_id_hex[:8],
            _progress_bar(20, snapshot.progress_ratio),
            f"{snapshot.received_chunks}/{snapshot.total_chunks} ({percent:5.1f}%)",
            _format_bps(rate_bps),
            str(snapshot.range_count),
            style="bold black on cyan" if index == selected_index else "",
        )

    if not snapshots:
        table.add_row("No active transfers", "-", "-", "-", "-", "-")

    help_line = Text("q=quit  r=reset counters", style="grey62")
    header = Panel(
        Group(
            Text("Space Sync Receiver Monitor", style="bold"),
            Text(f"output_dir={output_dir}", style="grey70"),
            summary,
            beacon_summary,
            help_line,
        ),
        border_style="blue",
        padding=(0, 1),
    )
    list_panel = Panel(
        table,
        border_style="cyan",
        padding=(0, 1),
        title="Active Transfers (Up/Down or j/k to select)",
    )

    selected = snapshots[selected_index] if snapshots else None
    if selected is None:
        detail_group = Group(Text("No active transfer selected.", style="grey62"))
    else:
        detail_header = Text()
        detail_header.append(selected.file_name or "<unknown>", style="bold")
        detail_header.append(f"  id={selected.transfer_id_hex[:8]}  ", style="grey70")
        detail_header.append(
            f"{selected.received_chunks}/{selected.total_chunks} chunks  "
            f"{selected.progress_ratio * 100.0:5.1f}%",
            style="cyan",
        )
        if selected.backfill_chunks > 0:
            detail_header.append(f"  repairs={selected.backfill_chunks}", style="red")
        detail_map = _build_hole_map_2d(
            selected,
            width=_DETAIL_MAP_WIDTH,
            height=_DETAIL_MAP_HEIGHT,
        )
        legend = Text("Legend: ▣ cursor  █ full  ▒ partial  · missing", style="grey62")
        detail_group = Group(detail_header, detail_map, legend)

    detail_panel = Panel(
        detail_group,
        border_style="magenta",
        padding=(0, 1),
        title="Selected Transfer Hole Map (2D)",
    )
    return Group(header, list_panel, detail_panel)


class _KeyReader:
    def __init__(self) -> None:
        self._fd = sys.stdin.fileno() if sys.stdin.isatty() else None
        self._saved_attrs: _TermiosAttrs | None = None

    def __enter__(self) -> _KeyReader:
        if self._fd is None:
            return self
        self._saved_attrs = cast(_TermiosAttrs, termios.tcgetattr(self._fd))
        tty.setcbreak(self._fd)
        return self

    def __exit__(self, *_args: object) -> None:
        if self._fd is None or self._saved_attrs is None:
            return
        termios.tcsetattr(self._fd, termios.TCSADRAIN, self._saved_attrs)

    def poll(self, timeout_s: float = 0.0) -> str | None:
        if self._fd is None:
            return None
        ready, _, _ = select.select([self._fd], [], [], max(0.0, timeout_s))
        if not ready:
            return None
        try:
            value = os.read(self._fd, 1)
        except OSError:
            return None
        if not value:
            return None
        if value == b"\x1b":
            sequence = b""
            deadline = time.monotonic() + 0.05
            while True:
                timeout_s = max(0.0, deadline - time.monotonic())
                more_ready, _, _ = select.select([self._fd], [], [], timeout_s)
                if not more_ready:
                    break
                try:
                    sequence += os.read(self._fd, 1)
                except OSError:
                    break
                if len(sequence) >= 2:
                    break
            if sequence.startswith(b"[A"):
                return "up"
            if sequence.startswith(b"[B"):
                return "down"
            return None
        decoded = value.decode(errors="ignore")
        if decoded.lower() in {"q"}:
            return "quit"
        if decoded.lower() in {"k"}:
            return "up"
        if decoded.lower() in {"j"}:
            return "down"
        if decoded.lower() in {"r"}:
            return "reset"
        return None


def _open_monitor_ipc_socket(path: Path | None) -> socket.socket | None:
    if path is None:
        return None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sock.bind(str(path))
        sock.setblocking(False)
        return sock
    except OSError:
        return None


def run_monitor_tui(
    output_dir: Path,
    refresh_interval_s: float,
    monitor_ipc_socket: Path | None = None,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    refresh_interval_s = max(0.1, refresh_interval_s)
    transfer_history: dict[str, deque[tuple[float, int]]] = {}
    throughput_bps: dict[str, float] = {}
    completed_count = 0
    completed_size = 0
    last_completed_scan_s = 0.0
    selected_index = 0
    snapshots: list[TransferSnapshot] = []
    previous_active_ids: set[str] = set()
    last_backfill_by_id: dict[str, int] = {}
    cumulative_repairs: int = 0
    displayed_mode: tuple[str, str] = ("IDLE", "grey58")
    displayed_mode_since_s = time.monotonic()
    last_feedback_seen_s = float("-inf")
    next_refresh_s = 0.0
    render_needed = True
    ipc_sock = _open_monitor_ipc_socket(monitor_ipc_socket)
    with _KeyReader() as keys:
        try:
            with Live(auto_refresh=False, screen=True) as live:
                while True:
                    now = time.monotonic()
                    if now >= next_refresh_s:
                        snapshots = _read_transfer_snapshots(output_dir)
                        snapshots = _merge_monitor_ipc_events(
                            snapshots,
                            _drain_monitor_ipc_events(ipc_sock),
                        )
                        activity_bytes: dict[str, int] = {}
                        scan_interval_s = (
                            _COMPLETED_SCAN_ACTIVE_INTERVAL_S
                            if snapshots
                            else _COMPLETED_SCAN_IDLE_INTERVAL_S
                        )
                        if (
                            last_completed_scan_s <= 0
                            or now - last_completed_scan_s >= scan_interval_s
                        ):
                            completed_count, completed_size = _count_completed_files(output_dir)
                            last_completed_scan_s = now
                        if snapshots:
                            selected_index = max(0, min(selected_index, len(snapshots) - 1))
                        else:
                            selected_index = 0
                        active_ids = {snapshot.transfer_id_hex for snapshot in snapshots}
                        new_active_ids = active_ids - previous_active_ids
                        for snapshot in snapshots:
                            sample = (
                                now,
                                snapshot.received_chunks * snapshot.chunk_size,
                            )
                            history = transfer_history.setdefault(snapshot.transfer_id_hex, deque())
                            latest_bytes = history[-1][1] if history else None
                            history.append(sample)
                            if latest_bytes is not None:
                                delta = sample[1] - latest_bytes
                                if delta > 0:
                                    activity_bytes[snapshot.transfer_id_hex] = delta
                            cutoff = now - _RATE_WINDOW_S
                            while len(history) > 2 and history[0][0] < cutoff:
                                history.popleft()
                            if len(history) >= 2:
                                oldest_time_s, oldest_bytes = history[0]
                                newest_time_s, newest_bytes = history[-1]
                                delta_s = newest_time_s - oldest_time_s
                                delta_bytes = newest_bytes - oldest_bytes
                                if delta_s > 0 and delta_bytes > 0:
                                    throughput_bps[snapshot.transfer_id_hex] = (
                                        delta_bytes * 8
                                    ) / delta_s
                        stale_ids = [item for item in transfer_history if item not in active_ids]
                        for stale_id in stale_ids:
                            transfer_history.pop(stale_id, None)
                            throughput_bps.pop(stale_id, None)
                            cumulative_repairs += last_backfill_by_id.pop(stale_id, 0)
                        for snapshot in snapshots:
                            last_backfill_by_id[snapshot.transfer_id_hex] = snapshot.backfill_chunks
                        selected_index = _autoselect_new_transfer_index(
                            snapshots,
                            selected_index=selected_index,
                            new_transfer_ids=new_active_ids,
                            activity_bytes=activity_bytes,
                            throughput_bps=throughput_bps,
                        )
                        previous_active_ids = active_ids
                        candidate_mode = _estimate_overall_mode(snapshots)
                        (
                            displayed_mode,
                            displayed_mode_since_s,
                            last_feedback_seen_s,
                        ) = _stabilize_overall_mode(
                            displayed_mode=displayed_mode,
                            candidate_mode=candidate_mode,
                            now_s=now,
                            displayed_since_s=displayed_mode_since_s,
                            last_feedback_seen_s=last_feedback_seen_s,
                        )
                        next_refresh_s = now + refresh_interval_s
                        render_needed = True
                    poll_timeout_s = 0.0 if render_needed else min(
                        _INPUT_POLL_INTERVAL_S,
                        max(0.0, next_refresh_s - time.monotonic()),
                    )
                    key = keys.poll(timeout_s=poll_timeout_s)
                    if key == "quit":
                        return 0
                    if key == "reset":
                        cumulative_repairs = 0
                        last_backfill_by_id.clear()
                        render_needed = True
                    if key == "up":
                        selected_index -= 1
                        render_needed = True
                    elif key == "down":
                        selected_index += 1
                        render_needed = True
                    if snapshots:
                        selected_index = max(0, min(selected_index, len(snapshots) - 1))
                    else:
                        selected_index = 0
                    if not render_needed:
                        continue
                    live.update(
                        _render_monitor(
                            output_dir=output_dir,
                            snapshots=snapshots,
                            throughput_bps=throughput_bps,
                            selected_index=selected_index,
                            completed_count=completed_count,
                            completed_size=completed_size,
                            cumulative_repairs=cumulative_repairs,
                            overall_mode=displayed_mode,
                        ),
                        refresh=True,
                    )
                    render_needed = False
        finally:
            if ipc_sock is not None:
                try:
                    if monitor_ipc_socket is not None and monitor_ipc_socket.exists():
                        monitor_ipc_socket.unlink()
                except OSError:
                    pass
                try:
                    ipc_sock.close()
                except OSError:
                    pass
