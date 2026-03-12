from __future__ import annotations

import json
import math
import os
import select
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


def _estimate_overall_mode(snapshots: list[TransferSnapshot]) -> tuple[str, str]:
    if not snapshots:
        return ("IDLE", "grey58")
    modes = [_estimate_transfer_mode(snapshot)[0] for snapshot in snapshots]
    if "FEEDBACK" in modes:
        return ("FEEDBACK", "magenta")
    if "NO-FEEDBACK" in modes:
        return ("NO-FEEDBACK", "yellow")
    if all(mode == "EMPTY" for mode in modes):
        return ("EMPTY", "grey58")
    if all(mode in {"COMPLETE", "EMPTY"} for mode in modes):
        return ("COMPLETE", "green")
    return ("NO-FEEDBACK", "yellow")


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
            )
        )
    snapshots.sort(key=lambda item: (item.file_name, item.transfer_id_hex))
    return snapshots


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


def _render_monitor(
    *,
    output_dir: Path,
    snapshots: list[TransferSnapshot],
    throughput_bps: dict[str, float],
    selected_index: int,
    completed_count: int,
    completed_size: int,
) -> Group:
    overall_mode_label, overall_mode_style = _estimate_overall_mode(snapshots)
    summary = Text()
    summary.append("active=", style="bold")
    summary.append(str(len(snapshots)), style="cyan")
    summary.append("  completed_files=", style="bold")
    summary.append(str(completed_count), style="cyan")
    summary.append("  completed_bytes=", style="bold")
    summary.append(_format_bytes(completed_size), style="cyan")
    summary.append("  mode=", style="bold")
    summary.append(overall_mode_label, style=overall_mode_style)

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

    help_line = Text("Press q to quit", style="grey62")
    header = Panel(
        Group(
            Text("Space Sync Receiver Monitor", style="bold"),
            Text(f"output_dir={output_dir}", style="grey70"),
            summary,
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
        return None


def run_monitor_tui(output_dir: Path, refresh_interval_s: float) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    refresh_interval_s = max(0.1, refresh_interval_s)
    transfer_history: dict[str, deque[tuple[float, int]]] = {}
    throughput_bps: dict[str, float] = {}
    completed_count = 0
    completed_size = 0
    last_completed_scan_s = 0.0
    selected_index = 0
    snapshots: list[TransferSnapshot] = []
    next_refresh_s = 0.0
    render_needed = True
    with _KeyReader() as keys:
        with Live(auto_refresh=False, screen=True) as live:
            while True:
                now = time.monotonic()
                if now >= next_refresh_s:
                    snapshots = _read_transfer_snapshots(output_dir)
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
                    for snapshot in snapshots:
                        sample = (
                            now,
                            snapshot.received_chunks * snapshot.chunk_size,
                        )
                        history = transfer_history.setdefault(snapshot.transfer_id_hex, deque())
                        history.append(sample)
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
                    next_refresh_s = now + refresh_interval_s
                    render_needed = True
                poll_timeout_s = 0.0 if render_needed else min(
                    _INPUT_POLL_INTERVAL_S,
                    max(0.0, next_refresh_s - time.monotonic()),
                )
                key = keys.poll(timeout_s=poll_timeout_s)
                if key == "quit":
                    return 0
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
                    ),
                    refresh=True,
                )
                render_needed = False
