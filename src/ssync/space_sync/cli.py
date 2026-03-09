from __future__ import annotations

import argparse
import fnmatch
import glob
import inspect
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path, PurePosixPath

from .monitor import run_monitor_tui
from .receiver import SpaceSyncReceiver
from .sender import SpaceSyncSender
from .types import ReceiverConfig, RemoteFileInfo, SenderConfig


def _default_log_level() -> str:
    return os.getenv("SSYNC_LOG_LEVEL", "WARNING")


def _add_log_level_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        default=_default_log_level(),
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Runtime logging level (or set SSYNC_LOG_LEVEL)",
    )


def _configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Space Sync UDP file transport prototype")
    subparsers = parser.add_subparsers(dest="command", required=True)

    recv = subparsers.add_parser("receive", help="Run a Space Sync receiver")
    recv.add_argument("--bind-host", default="127.0.0.1")
    recv.add_argument("--bind-port", type=int, default=9000)
    recv.add_argument("--output-dir", type=Path, default=Path("./received"))
    recv.add_argument("--feedback", action="store_true", help="Enable repair feedback")
    recv.add_argument(
        "--keep-part-files-on-complete",
        action="store_true",
        help="Keep .part files after successful completion (debugging)",
    )
    recv.add_argument(
        "--status-repeat",
        type=int,
        default=3,
        help="How many times status is repeated when feedback is enabled",
    )
    recv.add_argument(
        "--periodic-repair-request-s",
        type=float,
        default=0.5,
        help="Periodic repair request interval while transfer is in progress",
    )
    recv.add_argument(
        "--periodic-repair-min-seen-chunks",
        type=int,
        default=32,
        help="Minimum received chunk frontier before periodic repair requests",
    )
    recv.add_argument(
        "--max-repair-chunks-per-request",
        type=int,
        default=256,
        help="Cap chunks requested in each repair request (0 means unlimited)",
    )
    recv.add_argument(
        "--repair-request-cooldown-s",
        type=float,
        default=0.2,
        help="Minimum delay before re-requesting unchanged missing ranges after REPAIR_DONE",
    )
    recv.add_argument(
        "--repair-request-inflight-timeout-s",
        type=float,
        default=1.5,
        help="Timeout before resending a repair request still considered in-flight",
    )
    recv.add_argument(
        "--transfer-inactivity-timeout-s",
        type=float,
        default=10.0,
        help="Finalize transfer as incomplete after FIN + inactivity timeout",
    )
    recv.add_argument(
        "--socket-rcvbuf-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="OS socket receive buffer size in bytes",
    )
    recv.add_argument(
        "--journal-flush-interval-s",
        type=float,
        default=0.5,
        help="Receiver journal flush interval in seconds (0 flushes every update)",
    )
    recv.add_argument(
        "--beacon-interval-s",
        type=float,
        default=1.0,
        help="Receiver beacon interval in seconds (0 disables beacons)",
    )
    _add_log_level_arg(recv)

    server = subparsers.add_parser(
        "server",
        help="Run a destination server for rsync-like ssync sync operations",
    )
    _add_server_args(server)
    ssyncd = subparsers.add_parser(
        "ssyncd",
        help="Alias for the Space Sync destination server daemon",
    )
    _add_server_args(ssyncd)

    send = subparsers.add_parser("send", help="Send file(s) over Space Sync")
    send.add_argument("files", nargs="+")
    send.add_argument("--dest-host", default="127.0.0.1")
    send.add_argument("--dest-port", type=int, default=9000)
    send.add_argument("--chunk-size", type=int, default=1024)
    send.add_argument("--manifest-repeats", type=int, default=3)
    send.add_argument("--feedback", action="store_true", help="Enable repair flow")
    send.add_argument("--feedback-wait-s", type=float, default=2.0)
    send.add_argument(
        "--max-repair-rounds",
        type=int,
        default=32,
        help="Post-FIN repair rounds (0 means unlimited until timeout/complete)",
    )
    send.add_argument("--max-feedback-idle-timeouts", type=int, default=2)
    send.add_argument(
        "--drop-every-nth-data",
        type=int,
        default=0,
        help="Test helper: drop every nth data frame",
    )
    send.add_argument("--inter-packet-delay-s", type=float, default=0.0002)
    send.add_argument(
        "--max-data-rate-bps",
        type=int,
        default=0,
        help="Throttle payload transmit rate in bits/sec (0 means unlimited)",
    )
    send.add_argument(
        "--midstream-repair-max-rounds-per-poll",
        type=int,
        default=1,
        help="Maximum repair requests handled per midstream polling pass (0 unlimited)",
    )
    send.add_argument(
        "--midstream-repair-max-chunks-per-poll",
        type=int,
        default=512,
        help="Maximum repair chunks sent per midstream polling pass (0 unlimited)",
    )
    send.add_argument(
        "--repair-duplicate-suppression-s",
        type=float,
        default=0.2,
        help="Suppress servicing identical repair requests within this interval",
    )
    send.add_argument(
        "--beacon-interval-s",
        type=float,
        default=1.0,
        help="Sender beacon interval in seconds (0 disables beacons)",
    )
    send.add_argument("--json", action="store_true", dest="json_output")
    _add_log_level_arg(send)

    sync = subparsers.add_parser(
        "sync",
        help="Compatibility alias for rsync-like sync behavior",
    )
    _add_sync_args(sync)
    _add_log_level_arg(sync)

    monitor = subparsers.add_parser(
        "monitor",
        help="Run a TUI monitor for receiver transfer progress",
    )
    monitor.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./received"),
        help="Receiver output directory containing .ssync-journal.json",
    )
    monitor.add_argument(
        "--refresh-interval-s",
        type=float,
        default=0.5,
        help="TUI refresh interval in seconds",
    )
    _add_log_level_arg(monitor)
    return parser


def _build_rsync_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Space Sync rsync-like file synchronization")
    _add_sync_args(parser)
    _add_log_level_arg(parser)
    return parser


def _build_ssyncd_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Space Sync destination server daemon")
    _add_server_args(parser)
    return parser


def _add_server_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bind-host", default="0.0.0.0")
    parser.add_argument("--bind-port", type=int, default=9000)
    parser.add_argument(
        "--root-dir",
        type=Path,
        default=Path("./received"),
        help="Root directory where incoming files are written",
    )
    parser.add_argument(
        "--feedback",
        action="store_true",
        default=True,
        help="Enable repair feedback (default: enabled)",
    )
    parser.add_argument(
        "--no-feedback",
        action="store_false",
        dest="feedback",
        help="Disable feedback for open-loop only operation",
    )
    parser.add_argument(
        "--keep-part-files-on-complete",
        action="store_true",
        help="Keep .part files after successful completion (debugging)",
    )
    parser.add_argument(
        "--status-repeat",
        type=int,
        default=3,
        help="How many times status is repeated when feedback is enabled",
    )
    parser.add_argument(
        "--periodic-repair-request-s",
        type=float,
        default=0.5,
        help="Periodic repair request interval while transfer is in progress",
    )
    parser.add_argument(
        "--periodic-repair-min-seen-chunks",
        type=int,
        default=32,
        help="Minimum received chunk frontier before periodic repair requests",
    )
    parser.add_argument(
        "--max-repair-chunks-per-request",
        type=int,
        default=256,
        help="Cap chunks requested in each repair request (0 means unlimited)",
    )
    parser.add_argument(
        "--repair-request-cooldown-s",
        type=float,
        default=0.2,
        help="Minimum delay before re-requesting unchanged missing ranges after REPAIR_DONE",
    )
    parser.add_argument(
        "--repair-request-inflight-timeout-s",
        type=float,
        default=1.5,
        help="Timeout before resending a repair request still considered in-flight",
    )
    parser.add_argument(
        "--transfer-inactivity-timeout-s",
        type=float,
        default=10.0,
        help="Finalize transfer as incomplete after FIN + inactivity timeout",
    )
    parser.add_argument(
        "--socket-rcvbuf-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="OS socket receive buffer size in bytes",
    )
    parser.add_argument(
        "--journal-flush-interval-s",
        type=float,
        default=0.5,
        help="Receiver journal flush interval in seconds (0 flushes every update)",
    )
    parser.add_argument(
        "--beacon-interval-s",
        type=float,
        default=1.0,
        help="Receiver beacon interval in seconds (0 disables beacons)",
    )
    _add_log_level_arg(parser)


def _add_sync_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "paths",
        nargs="+",
        help="Source path(s) followed by destination in host:path form",
    )
    parser.add_argument(
        "-D",
        "--destination",
        action="append",
        default=[],
        dest="destinations",
        help="Additional destination in host:path form (repeatable)",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recurse into source directories",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Show actions without sending data",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity")
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Include only paths matching glob",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Exclude paths matching glob",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Reserved for future delete semantics",
    )
    parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        help="Query destination metadata and skip unchanged files",
    )
    parser.add_argument(
        "--checksum",
        action="store_true",
        help="Use checksum (with --skip-unchanged) for unchanged checks",
    )
    parser.add_argument("--dest-port", type=int, default=9000)
    parser.add_argument("--chunk-size", type=int, default=1024)
    parser.add_argument("--manifest-repeats", type=int, default=3)
    parser.add_argument(
        "--feedback",
        action="store_true",
        default=True,
        help="Enable repair flow (default: enabled)",
    )
    parser.add_argument(
        "--no-feedback",
        action="store_false",
        dest="feedback",
        help="Disable feedback/repair flow",
    )
    parser.add_argument("--feedback-wait-s", type=float, default=2.0)
    parser.add_argument(
        "--max-repair-rounds",
        type=int,
        default=32,
        help="Post-FIN repair rounds (0 means unlimited until timeout/complete)",
    )
    parser.add_argument("--max-feedback-idle-timeouts", type=int, default=2)
    parser.add_argument(
        "--drop-every-nth-data",
        type=int,
        default=0,
        help="Test helper: drop every nth data frame",
    )
    parser.add_argument("--inter-packet-delay-s", type=float, default=0.0002)
    parser.add_argument(
        "--max-data-rate-bps",
        type=int,
        default=0,
        help="Throttle payload transmit rate in bits/sec (0 means unlimited)",
    )
    parser.add_argument(
        "--midstream-repair-max-rounds-per-poll",
        type=int,
        default=1,
        help="Maximum repair requests handled per midstream polling pass (0 unlimited)",
    )
    parser.add_argument(
        "--midstream-repair-max-chunks-per-poll",
        type=int,
        default=512,
        help="Maximum repair chunks sent per midstream polling pass (0 unlimited)",
    )
    parser.add_argument(
        "--repair-duplicate-suppression-s",
        type=float,
        default=0.2,
        help="Suppress servicing identical repair requests within this interval",
    )
    parser.add_argument(
        "--beacon-interval-s",
        type=float,
        default=1.0,
        help="Sender beacon interval in seconds (0 disables beacons)",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path(".ssync-open-loop-state.json"),
        help="Persistent send-state file used for open-loop retransmission ordering",
    )
    parser.add_argument(
        "--open-loop-max-rounds",
        type=int,
        default=0,
        help="Open-loop rounds to run (0 means run continuously)",
    )
    parser.add_argument("--json", action="store_true", dest="json_output")


def _run_receiver_common(
    *,
    bind_host: str,
    bind_port: int,
    output_dir: Path,
    feedback: bool,
    keep_part_files_on_complete: bool,
    status_repeat: int,
    periodic_repair_request_s: float,
    periodic_repair_min_seen_chunks: int,
    max_repair_chunks_per_request: int,
    repair_request_cooldown_s: float,
    repair_request_inflight_timeout_s: float,
    transfer_inactivity_timeout_s: float,
    socket_rcvbuf_bytes: int,
    journal_flush_interval_s: float,
    beacon_interval_s: float,
    banner: str,
) -> int:
    receiver = SpaceSyncReceiver(
        bind_host=bind_host,
        bind_port=bind_port,
        config=ReceiverConfig(
            output_dir=output_dir,
            enable_feedback=feedback,
            keep_part_files_on_complete=keep_part_files_on_complete,
            status_repeat=max(1, status_repeat),
            periodic_repair_request_s=max(0.0, periodic_repair_request_s),
            periodic_repair_min_seen_chunks=max(1, periodic_repair_min_seen_chunks),
            max_repair_chunks_per_request=max(0, max_repair_chunks_per_request),
            repair_request_cooldown_s=max(0.0, repair_request_cooldown_s),
            repair_request_inflight_timeout_s=max(0.0, repair_request_inflight_timeout_s),
            transfer_inactivity_timeout_s=max(0.0, transfer_inactivity_timeout_s),
            socket_rcvbuf_bytes=max(0, socket_rcvbuf_bytes),
            journal_flush_interval_s=max(0.0, journal_flush_interval_s),
            beacon_interval_s=max(0.0, beacon_interval_s),
        ),
    )
    receiver.start()
    should_stop = False

    def _handle_signal(_signum: int, _frame: object) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    print(banner)
    try:
        while not should_stop:
            time.sleep(0.25)
    finally:
        receiver.stop()
        print("Receiver stopped")
    return 0


def _run_receiver(args: argparse.Namespace) -> int:
    return _run_receiver_common(
        bind_host=args.bind_host,
        bind_port=args.bind_port,
        output_dir=args.output_dir,
        feedback=args.feedback,
        keep_part_files_on_complete=args.keep_part_files_on_complete,
        status_repeat=args.status_repeat,
        periodic_repair_request_s=args.periodic_repair_request_s,
        periodic_repair_min_seen_chunks=args.periodic_repair_min_seen_chunks,
        max_repair_chunks_per_request=args.max_repair_chunks_per_request,
        repair_request_cooldown_s=args.repair_request_cooldown_s,
        repair_request_inflight_timeout_s=args.repair_request_inflight_timeout_s,
        transfer_inactivity_timeout_s=args.transfer_inactivity_timeout_s,
        socket_rcvbuf_bytes=args.socket_rcvbuf_bytes,
        journal_flush_interval_s=args.journal_flush_interval_s,
        beacon_interval_s=args.beacon_interval_s,
        banner=f"Space Sync receiver listening on {args.bind_host}:{args.bind_port}",
    )


def _run_server(args: argparse.Namespace) -> int:
    return _run_receiver_common(
        bind_host=args.bind_host,
        bind_port=args.bind_port,
        output_dir=args.root_dir,
        feedback=args.feedback,
        keep_part_files_on_complete=args.keep_part_files_on_complete,
        status_repeat=args.status_repeat,
        periodic_repair_request_s=args.periodic_repair_request_s,
        periodic_repair_min_seen_chunks=args.periodic_repair_min_seen_chunks,
        max_repair_chunks_per_request=args.max_repair_chunks_per_request,
        repair_request_cooldown_s=args.repair_request_cooldown_s,
        repair_request_inflight_timeout_s=args.repair_request_inflight_timeout_s,
        transfer_inactivity_timeout_s=args.transfer_inactivity_timeout_s,
        socket_rcvbuf_bytes=args.socket_rcvbuf_bytes,
        journal_flush_interval_s=args.journal_flush_interval_s,
        beacon_interval_s=args.beacon_interval_s,
        banner=(
            "Space Sync server listening on "
            f"{args.bind_host}:{args.bind_port} root={args.root_dir}"
        ),
    )


def _run_sender(args: argparse.Namespace) -> int:
    sender = SpaceSyncSender(
        config=SenderConfig(
            chunk_size=args.chunk_size,
            manifest_repeats=args.manifest_repeats,
            inter_packet_delay_s=args.inter_packet_delay_s,
            enable_feedback=args.feedback,
            feedback_wait_s=args.feedback_wait_s,
            max_repair_rounds=args.max_repair_rounds,
            max_feedback_idle_timeouts=args.max_feedback_idle_timeouts,
            drop_every_nth_data=args.drop_every_nth_data,
            max_data_rate_bps=max(0, args.max_data_rate_bps),
            midstream_repair_max_rounds_per_poll=max(
                0, args.midstream_repair_max_rounds_per_poll
            ),
            midstream_repair_max_chunks_per_poll=max(
                0, args.midstream_repair_max_chunks_per_poll
            ),
            repair_duplicate_suppression_s=max(0.0, args.repair_duplicate_suppression_s),
            beacon_interval_s=max(0.0, args.beacon_interval_s),
        )
    )
    try:
        files = _expand_sync_sources(args.files)
    except ValueError as exc:
        print(f"send error: {exc}")
        return 2
    if not files:
        print("send error: no files selected")
        return 2

    results: list[dict[str, object]] = []
    failed = 0
    should_stop = False

    def _handle_signal(_signum: int, _frame: object) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    send_file_params = inspect.signature(sender.send_file).parameters
    supports_stop_requested = "stop_requested" in send_file_params
    for file_path in files:
        if should_stop:
            break
        if not file_path.is_file():
            print(f"send error: not a file: {file_path}")
            return 2
        if supports_stop_requested:
            result = sender.send_file(
                file_path=file_path,
                destination_host=args.dest_host,
                destination_port=args.dest_port,
                stop_requested=lambda: should_stop,
            )
        else:
            result = sender.send_file(
                file_path=file_path,
                destination_host=args.dest_host,
                destination_port=args.dest_port,
            )
        if not result.completed:
            failed += 1
        entry = {
            "source": str(file_path),
            "transfer_id": result.transfer_id_hex,
            "chunks": result.total_chunks,
            "repaired": result.repaired_chunks,
            "rounds": result.repair_rounds,
            "completed": result.completed,
        }
        results.append(entry)
        if not args.json_output:
            print(
                f"source={file_path} transfer_id={result.transfer_id_hex} "
                f"chunks={result.total_chunks} repaired={result.repaired_chunks} "
                f"rounds={result.repair_rounds} completed={result.completed}"
            )

    if args.json_output:
        if len(results) == 1:
            single = results[0]
            print(
                json.dumps(
                    {
                        "transfer_id": single["transfer_id"],
                        "chunks": single["chunks"],
                        "repaired": single["repaired"],
                        "rounds": single["rounds"],
                        "completed": single["completed"],
                    }
                )
            )
        else:
            print(
                json.dumps(
                    {
                        "summary": {
                            "files": len(results),
                            "incomplete": failed,
                            "success": failed == 0,
                        },
                        "results": results,
                    }
                )
            )
    return 0 if failed == 0 else 1


def _parse_destination(destination: str) -> tuple[str, str]:
    host, sep, remote_path = destination.partition(":")
    if not sep or not host or not remote_path:
        raise ValueError("destination must be in host:path format")
    if "@" in host:
        _, host = host.split("@", 1)
        if not host:
            raise ValueError("destination host must not be empty")
    if remote_path.startswith("/"):
        raise ValueError("destination path must be relative to the server root")
    return host, remote_path


def _path_matches(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def _collect_sync_items(
    source: Path,
    remote_root: str,
    *,
    recursive: bool,
    includes: list[str],
    excludes: list[str],
    source_prefix: str | None = None,
) -> list[tuple[Path, str]]:
    source = source.resolve()
    remote_root_path = PurePosixPath(remote_root)
    if source.is_file():
        if source_prefix is not None:
            remote_name = str(remote_root_path / source_prefix)
        elif remote_root.endswith("/"):
            remote_name = str(remote_root_path / source.name)
        else:
            remote_name = str(remote_root_path)
        return [(source, remote_name)]
    if source.is_dir():
        if not recursive:
            raise ValueError("source is a directory; use -r/--recursive")
        files = sorted(path for path in source.rglob("*") if path.is_file())
        items: list[tuple[Path, str]] = []
        for file_path in files:
            relative = file_path.relative_to(source).as_posix()
            if includes and not _path_matches(relative, includes):
                continue
            if excludes and _path_matches(relative, excludes):
                continue
            remote_base = remote_root_path / source_prefix if source_prefix else remote_root_path
            remote_name = str(remote_base / relative)
            items.append((file_path, remote_name))
        return items
    raise ValueError(f"source not found: {source}")


def _expand_sync_sources(source_args: list[str]) -> list[Path]:
    expanded: list[Path] = []
    seen: set[Path] = set()
    for value in source_args:
        matched_paths: list[Path]
        if glob.has_magic(value):
            matches = [Path(match).resolve() for match in glob.glob(value, recursive=True)]
            if not matches:
                raise ValueError(f"source pattern matched no paths: {value}")
            matched_paths = sorted(matches)
        else:
            matched_paths = [Path(value).resolve()]
        for matched in matched_paths:
            if matched in seen:
                continue
            seen.add(matched)
            expanded.append(matched)
    return expanded


def _is_unchanged(
    source_file: Path,
    remote_info: RemoteFileInfo | None,
    *,
    checksum: bool,
) -> bool:
    if remote_info is None:
        return False
    if not remote_info.exists:
        return False
    source_stat = source_file.stat()
    if source_stat.st_size != remote_info.size:
        return False
    if checksum:
        source_hash = SpaceSyncSender.local_file_checksum(source_file)
        return remote_info.sha256 == source_hash
    return source_stat.st_mtime_ns == remote_info.mtime_ns


def _retransmission_key(
    *,
    destination_host: str,
    destination_port: int,
    remote_name: str,
) -> str:
    return f"{destination_host}:{destination_port}:{remote_name}"


def _load_open_loop_state(state_file: Path) -> dict[str, int]:
    if not state_file.exists():
        return {}
    try:
        raw = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    counts = raw.get("retransmission_counts")
    if not isinstance(counts, dict):
        return {}
    normalized: dict[str, int] = {}
    for key, value in counts.items():
        if not isinstance(key, str):
            continue
        if not isinstance(value, int):
            continue
        if value < 0:
            continue
        normalized[key] = value
    return normalized


def _save_open_loop_state(state_file: Path, counts: dict[str, int]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_file.with_suffix(state_file.suffix + ".tmp")
    temp_path.write_text(
        json.dumps({"version": 1, "retransmission_counts": counts}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(state_file)


def _order_items_for_open_loop(
    items: list[tuple[Path, str]],
    *,
    destination_host: str,
    destination_port: int,
    counts: dict[str, int],
) -> list[tuple[Path, str]]:
    return sorted(
        items,
        key=lambda item: (
            counts.get(
                _retransmission_key(
                    destination_host=destination_host,
                    destination_port=destination_port,
                    remote_name=item[1],
                ),
                0,
            ),
            item[1],
        ),
    )


def _run_sync(args: argparse.Namespace) -> int:
    if len(args.paths) < 2:
        print("sync error: expected at least one source and one destination")
        return 2
    source_args = args.paths[:-1]
    destination_args = [args.paths[-1], *args.destinations]
    try:
        sources = _expand_sync_sources(source_args)
        sync_plans: list[tuple[str, list[tuple[Path, str]]]] = []
        for destination in destination_args:
            destination_host, remote_root = _parse_destination(destination)
            if len(sources) > 1 and not remote_root.endswith("/"):
                raise ValueError(
                    "destination must end with '/' when syncing multiple source paths"
                )
            destination_items: list[tuple[Path, str]] = []
            for source in sources:
                source_prefix = source.name if len(sources) > 1 else None
                destination_items.extend(
                    _collect_sync_items(
                        source,
                        remote_root,
                        recursive=args.recursive,
                        includes=args.include,
                        excludes=args.exclude,
                        source_prefix=source_prefix,
                    )
                )
            sync_plans.append((destination_host, destination_items))
    except ValueError as exc:
        print(f"sync error: {exc}")
        return 2
    if args.delete:
        print("sync error: --delete is not implemented yet")
        return 2
    if args.checksum and not args.skip_unchanged:
        print("sync error: --checksum requires --skip-unchanged")
        return 2
    if not sync_plans or not any(items for _, items in sync_plans):
        print("sync error: source directory contains no files")
        return 2

    sender = SpaceSyncSender(
        config=SenderConfig(
            chunk_size=args.chunk_size,
            manifest_repeats=args.manifest_repeats,
            inter_packet_delay_s=args.inter_packet_delay_s,
            enable_feedback=args.feedback,
            feedback_wait_s=args.feedback_wait_s,
            max_repair_rounds=args.max_repair_rounds,
            max_feedback_idle_timeouts=args.max_feedback_idle_timeouts,
            drop_every_nth_data=args.drop_every_nth_data,
            max_data_rate_bps=max(0, args.max_data_rate_bps),
            midstream_repair_max_rounds_per_poll=max(
                0, args.midstream_repair_max_rounds_per_poll
            ),
            midstream_repair_max_chunks_per_poll=max(
                0, args.midstream_repair_max_chunks_per_poll
            ),
            repair_duplicate_suppression_s=max(0.0, args.repair_duplicate_suppression_s),
            beacon_interval_s=max(0.0, args.beacon_interval_s),
        )
    )

    failed = 0
    sent_count = 0
    skipped_count = 0
    dry_run_count = 0
    should_query_destination = bool(args.skip_unchanged)
    open_loop_mode = not args.feedback
    send_file_params = inspect.signature(sender.send_file).parameters
    supports_stop_requested = "stop_requested" in send_file_params
    if args.open_loop_max_rounds < 0:
        print("sync error: --open-loop-max-rounds must be >= 0")
        return 2
    open_loop_state = _load_open_loop_state(args.state_file) if open_loop_mode else {}
    round_index = 0
    should_stop = False

    def _handle_signal(_signum: int, _frame: object) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collect_item_results = (not open_loop_mode) or args.dry_run or args.open_loop_max_rounds > 0
    item_results: list[dict[str, object]] | None = [] if collect_item_results else None
    total_items = sum(len(items) for _, items in sync_plans)
    while True:
        round_index += 1
        for destination_host, items in sync_plans:
            ordered_items = (
                _order_items_for_open_loop(
                    items,
                    destination_host=destination_host,
                    destination_port=args.dest_port,
                    counts=open_loop_state,
                )
                if open_loop_mode
                else items
            )
            for source_file, remote_name in ordered_items:
                remote_info = None
                if should_query_destination:
                    try:
                        remote_info = sender.query_remote_file(
                            destination_host=destination_host,
                            destination_port=args.dest_port,
                            remote_name=remote_name,
                            include_checksum=args.checksum,
                        )
                    except (TimeoutError, ValueError):
                        remote_info = None

                if args.skip_unchanged and _is_unchanged(
                    source_file,
                    remote_info,
                    checksum=args.checksum,
                ):
                    status = "skipped"
                    skipped_count += 1
                    item_result = {
                        "status": status,
                        "source": str(source_file),
                        "destination": f"{destination_host}:{remote_name}",
                        "completed": True,
                    }
                elif args.dry_run:
                    status = "would-send"
                    dry_run_count += 1
                    item_result = {
                        "status": status,
                        "source": str(source_file),
                        "destination": f"{destination_host}:{remote_name}",
                        "completed": True,
                    }
                else:
                    if supports_stop_requested:
                        result = sender.send_file(
                            file_path=source_file,
                            destination_host=destination_host,
                            destination_port=args.dest_port,
                            remote_name=remote_name,
                            stop_requested=lambda: should_stop,
                        )
                    else:
                        result = sender.send_file(
                            file_path=source_file,
                            destination_host=destination_host,
                            destination_port=args.dest_port,
                            remote_name=remote_name,
                        )
                    status = "sent" if result.completed else "incomplete"
                    if not result.completed:
                        failed += 1
                    else:
                        sent_count += 1
                    item_result = {
                        "status": status,
                        "source": str(source_file),
                        "destination": f"{destination_host}:{remote_name}",
                        "transfer_id": result.transfer_id_hex,
                        "chunks": result.total_chunks,
                        "repaired": result.repaired_chunks,
                        "rounds": result.repair_rounds,
                        "completed": result.completed,
                    }
                    if open_loop_mode:
                        key = _retransmission_key(
                            destination_host=destination_host,
                            destination_port=args.dest_port,
                            remote_name=remote_name,
                        )
                        open_loop_state[key] = open_loop_state.get(key, 0) + 1
                        _save_open_loop_state(args.state_file, open_loop_state)

                if item_results is not None:
                    item_results.append(item_result)
                if args.verbose and not args.json_output:
                    print(
                        f"[{status}] {source_file} -> {destination_host}:{remote_name} "
                        f"completed={item_result.get('completed', False)}"
                    )

        if args.dry_run:
            break
        if should_stop:
            break
        if not open_loop_mode:
            break
        if args.open_loop_max_rounds > 0 and round_index >= args.open_loop_max_rounds:
            break

    if failed:
        if args.json_output:
            print(
                json.dumps(
                    {
                        "summary": {
                            "files": total_items,
                            "incomplete": failed,
                            "sent": sent_count,
                            "skipped": skipped_count,
                            "would_send": dry_run_count,
                            "success": False,
                        },
                        "results": item_results if item_results is not None else [],
                        "results_limited": item_results is None,
                    }
                )
            )
        else:
            print(f"sync completed with {failed} incomplete transfer(s)")
        return 1
    if args.json_output:
        print(
            json.dumps(
                {
                    "summary": {
                        "files": total_items,
                        "incomplete": 0,
                        "sent": sent_count,
                        "skipped": skipped_count,
                        "would_send": dry_run_count,
                        "success": True,
                    },
                    "results": item_results if item_results is not None else [],
                    "results_limited": item_results is None,
                }
            )
        )
    else:
        print(
            "sync complete: "
            f"files={total_items} sent={sent_count} skipped={skipped_count} "
            f"would_send={dry_run_count}"
        )
    return 0


def _run_monitor(args: argparse.Namespace) -> int:
    try:
        return run_monitor_tui(
            output_dir=args.output_dir,
            refresh_interval_s=args.refresh_interval_s,
        )
    except KeyboardInterrupt:
        return 0


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    subcommands = {"receive", "server", "ssyncd", "send", "sync", "monitor"}
    if argv and argv[0] in subcommands:
        parser = _build_parser()
        args = parser.parse_args(argv)
    else:
        parser = _build_rsync_parser()
        args = parser.parse_args(argv)
        args.command = "sync"
    _configure_logging(args.log_level)
    if args.command == "receive":
        return _run_receiver(args)
    if args.command in {"server", "ssyncd"}:
        return _run_server(args)
    if args.command == "send":
        return _run_sender(args)
    if args.command == "sync":
        return _run_sync(args)
    if args.command == "monitor":
        return _run_monitor(args)
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())


def ssyncd_main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = _build_ssyncd_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    return _run_server(args)

