from __future__ import annotations

import argparse
import collections
import dataclasses
import fnmatch
import glob
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
        "--adaptive-leading-hole-boost",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Dynamically boost repair request chunk budget for large leading holes",
    )
    recv.add_argument(
        "--leading-hole-start-threshold-chunks",
        type=int,
        default=512,
        help="Max starting chunk index for considering a leading-hole boost",
    )
    recv.add_argument(
        "--leading-hole-min-span-chunks",
        type=int,
        default=2048,
        help="Minimum first-missing-range span to trigger leading-hole boost",
    )
    recv.add_argument(
        "--leading-hole-boost-multiplier",
        type=int,
        default=4,
        help="Multiplier applied to max repair chunk budget during leading-hole boost",
    )
    recv.add_argument(
        "--leading-hole-max-repair-chunks-per-request",
        type=int,
        default=2048,
        help="Hard cap for boosted per-request repair chunk budget (0 means unlimited)",
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
    recv.add_argument(
        "--pre-metadata-max-pending-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="Max global bytes buffered for unknown-transfer DATA",
    )
    recv.add_argument(
        "--pre-metadata-max-pending-bytes-per-transfer",
        type=int,
        default=512 * 1024,
        help="Max bytes buffered per unknown transfer ID",
    )
    recv.add_argument(
        "--pre-metadata-max-pending-transfers",
        type=int,
        default=128,
        help="Max unknown transfer IDs tracked in pre-metadata buffer",
    )
    recv.add_argument(
        "--pre-metadata-ttl-s",
        type=float,
        default=30.0,
        help="TTL for buffered unknown-transfer DATA before eviction",
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
    send.add_argument(
        "--metadata-repeats",
        type=int,
        dest="manifest_repeats",
        help="Alias for --manifest-repeats",
    )
    send_feedback = send.add_mutually_exclusive_group()
    send_feedback.add_argument(
        "--feedback",
        action="store_const",
        const=True,
        default=None,
        dest="feedback",
        help="Force feedback/repair flow on",
    )
    send_feedback.add_argument(
        "--no-feedback",
        action="store_const",
        const=False,
        dest="feedback",
        help="Force feedback/repair flow off",
    )
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
    send.add_argument(
        "--periodic-metadata-interval-s",
        type=float,
        default=10.0,
        help="Send METADATA periodically during transfer (seconds, default 10, 0 disables)",
    )
    send.add_argument(
        "--periodic-metadata-every-n-chunks",
        type=int,
        default=0,
        help="Send METADATA every N data chunks during transfer (0 disables)",
    )
    send.add_argument(
        "--revisit-incomplete-passes",
        type=int,
        default=2,
        help="Maximum revisit attempts for each incomplete transfer (feedback mode)",
    )
    send.add_argument(
        "--revisit-max-rounds-per-pass",
        type=int,
        default=8,
        help="Max repair rounds handled during each revisit attempt (0 unlimited)",
    )
    send.add_argument(
        "--primary-feedback-max-rounds",
        type=int,
        default=0,
        help=(
            "Cap feedback repair rounds per primary file attempt in sync-style flows "
            "(0 disables)"
        ),
    )
    send.add_argument(
        "--primary-feedback-max-seconds",
        type=float,
        default=0.0,
        help=(
            "Cap wall-clock feedback servicing time per primary file attempt in "
            "sync-style flows (0 disables)"
        ),
    )
    send.add_argument("--json", action="store_true", dest="json_output")
    _add_log_level_arg(send)

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
        "--adaptive-leading-hole-boost",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Dynamically boost repair request chunk budget for large leading holes",
    )
    parser.add_argument(
        "--leading-hole-start-threshold-chunks",
        type=int,
        default=512,
        help="Max starting chunk index for considering a leading-hole boost",
    )
    parser.add_argument(
        "--leading-hole-min-span-chunks",
        type=int,
        default=2048,
        help="Minimum first-missing-range span to trigger leading-hole boost",
    )
    parser.add_argument(
        "--leading-hole-boost-multiplier",
        type=int,
        default=4,
        help="Multiplier applied to max repair chunk budget during leading-hole boost",
    )
    parser.add_argument(
        "--leading-hole-max-repair-chunks-per-request",
        type=int,
        default=2048,
        help="Hard cap for boosted per-request repair chunk budget (0 means unlimited)",
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
    parser.add_argument(
        "--pre-metadata-max-pending-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="Max global bytes buffered for unknown-transfer DATA",
    )
    parser.add_argument(
        "--pre-metadata-max-pending-bytes-per-transfer",
        type=int,
        default=512 * 1024,
        help="Max bytes buffered per unknown transfer ID",
    )
    parser.add_argument(
        "--pre-metadata-max-pending-transfers",
        type=int,
        default=128,
        help="Max unknown transfer IDs tracked in pre-metadata buffer",
    )
    parser.add_argument(
        "--pre-metadata-ttl-s",
        type=float,
        default=30.0,
        help="TTL for buffered unknown-transfer DATA before eviction",
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
        "--metadata-repeats",
        type=int,
        dest="manifest_repeats",
        help="Alias for --manifest-repeats",
    )
    feedback_group = parser.add_mutually_exclusive_group()
    feedback_group.add_argument(
        "--feedback",
        action="store_const",
        const=True,
        default=None,
        dest="feedback",
        help="Force feedback/repair flow on",
    )
    feedback_group.add_argument(
        "--no-feedback",
        action="store_const",
        const=False,
        dest="feedback",
        help="Force feedback/repair flow off",
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
        "--periodic-metadata-interval-s",
        type=float,
        default=10.0,
        help="Send METADATA periodically during transfer (seconds, default 10, 0 disables)",
    )
    parser.add_argument(
        "--periodic-metadata-every-n-chunks",
        type=int,
        default=0,
        help="Send METADATA every N data chunks during transfer (0 disables)",
    )
    parser.add_argument(
        "--revisit-incomplete-passes",
        type=int,
        default=2,
        help="Maximum revisit attempts for each incomplete transfer (feedback mode)",
    )
    parser.add_argument(
        "--revisit-max-rounds-per-pass",
        type=int,
        default=8,
        help="Max repair rounds handled during each revisit attempt (0 unlimited)",
    )
    parser.add_argument(
        "--primary-feedback-max-rounds",
        type=int,
        default=64,
        help=(
            "Cap feedback repair rounds per primary file attempt so revisits run "
            "regularly (0 disables)"
        ),
    )
    parser.add_argument(
        "--primary-feedback-max-seconds",
        type=float,
        default=8.0,
        help=(
            "Cap wall-clock feedback servicing time per primary file attempt so "
            "revisits run regularly (0 disables)"
        ),
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
    adaptive_leading_hole_boost: bool,
    leading_hole_start_threshold_chunks: int,
    leading_hole_min_span_chunks: int,
    leading_hole_boost_multiplier: int,
    leading_hole_max_repair_chunks_per_request: int,
    repair_request_cooldown_s: float,
    repair_request_inflight_timeout_s: float,
    transfer_inactivity_timeout_s: float,
    socket_rcvbuf_bytes: int,
    journal_flush_interval_s: float,
    beacon_interval_s: float,
    pre_metadata_max_pending_bytes: int,
    pre_metadata_max_pending_bytes_per_transfer: int,
    pre_metadata_max_pending_transfers: int,
    pre_metadata_ttl_s: float,
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
            adaptive_leading_hole_boost=adaptive_leading_hole_boost,
            leading_hole_start_threshold_chunks=max(0, leading_hole_start_threshold_chunks),
            leading_hole_min_span_chunks=max(1, leading_hole_min_span_chunks),
            leading_hole_boost_multiplier=max(1, leading_hole_boost_multiplier),
            leading_hole_max_repair_chunks_per_request=max(
                0,
                leading_hole_max_repair_chunks_per_request,
            ),
            repair_request_cooldown_s=max(0.0, repair_request_cooldown_s),
            repair_request_inflight_timeout_s=max(0.0, repair_request_inflight_timeout_s),
            transfer_inactivity_timeout_s=max(0.0, transfer_inactivity_timeout_s),
            socket_rcvbuf_bytes=max(0, socket_rcvbuf_bytes),
            journal_flush_interval_s=max(0.0, journal_flush_interval_s),
            beacon_interval_s=max(0.0, beacon_interval_s),
            pre_metadata_max_pending_bytes=max(0, pre_metadata_max_pending_bytes),
            pre_metadata_max_pending_bytes_per_transfer=max(
                0, pre_metadata_max_pending_bytes_per_transfer
            ),
            pre_metadata_max_pending_transfers=max(1, pre_metadata_max_pending_transfers),
            pre_metadata_ttl_s=max(0.0, pre_metadata_ttl_s),
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
        adaptive_leading_hole_boost=args.adaptive_leading_hole_boost,
        leading_hole_start_threshold_chunks=args.leading_hole_start_threshold_chunks,
        leading_hole_min_span_chunks=args.leading_hole_min_span_chunks,
        leading_hole_boost_multiplier=args.leading_hole_boost_multiplier,
        leading_hole_max_repair_chunks_per_request=args.leading_hole_max_repair_chunks_per_request,
        repair_request_cooldown_s=args.repair_request_cooldown_s,
        repair_request_inflight_timeout_s=args.repair_request_inflight_timeout_s,
        transfer_inactivity_timeout_s=args.transfer_inactivity_timeout_s,
        socket_rcvbuf_bytes=args.socket_rcvbuf_bytes,
        journal_flush_interval_s=args.journal_flush_interval_s,
        beacon_interval_s=args.beacon_interval_s,
        pre_metadata_max_pending_bytes=args.pre_metadata_max_pending_bytes,
        pre_metadata_max_pending_bytes_per_transfer=(
            args.pre_metadata_max_pending_bytes_per_transfer
        ),
        pre_metadata_max_pending_transfers=args.pre_metadata_max_pending_transfers,
        pre_metadata_ttl_s=args.pre_metadata_ttl_s,
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
        adaptive_leading_hole_boost=args.adaptive_leading_hole_boost,
        leading_hole_start_threshold_chunks=args.leading_hole_start_threshold_chunks,
        leading_hole_min_span_chunks=args.leading_hole_min_span_chunks,
        leading_hole_boost_multiplier=args.leading_hole_boost_multiplier,
        leading_hole_max_repair_chunks_per_request=args.leading_hole_max_repair_chunks_per_request,
        repair_request_cooldown_s=args.repair_request_cooldown_s,
        repair_request_inflight_timeout_s=args.repair_request_inflight_timeout_s,
        transfer_inactivity_timeout_s=args.transfer_inactivity_timeout_s,
        socket_rcvbuf_bytes=args.socket_rcvbuf_bytes,
        journal_flush_interval_s=args.journal_flush_interval_s,
        beacon_interval_s=args.beacon_interval_s,
        pre_metadata_max_pending_bytes=args.pre_metadata_max_pending_bytes,
        pre_metadata_max_pending_bytes_per_transfer=(
            args.pre_metadata_max_pending_bytes_per_transfer
        ),
        pre_metadata_max_pending_transfers=args.pre_metadata_max_pending_transfers,
        pre_metadata_ttl_s=args.pre_metadata_ttl_s,
        banner=(
            "Space Sync server listening on "
            f"{args.bind_host}:{args.bind_port} root={args.root_dir}"
        ),
    )


def _run_sender(args: argparse.Namespace) -> int:
    feedback_forced_on = args.feedback is True
    feedback_forced_off = args.feedback is False
    sender = SpaceSyncSender(
        config=SenderConfig(
            chunk_size=args.chunk_size,
            manifest_repeats=args.manifest_repeats,
            inter_packet_delay_s=args.inter_packet_delay_s,
            enable_feedback=feedback_forced_on,
            auto_feedback_discovery=not (feedback_forced_on or feedback_forced_off),
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
            periodic_metadata_interval_s=max(0.0, args.periodic_metadata_interval_s),
            periodic_metadata_every_n_chunks=max(0, args.periodic_metadata_every_n_chunks),
            revisit_incomplete_passes=max(0, args.revisit_incomplete_passes),
            revisit_max_rounds_per_pass=max(0, args.revisit_max_rounds_per_pass),
            primary_feedback_max_rounds=max(0, args.primary_feedback_max_rounds),
            primary_feedback_max_seconds=max(0.0, args.primary_feedback_max_seconds),
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
    for file_path in files:
        if should_stop:
            break
        if not file_path.is_file():
            print(f"send error: not a file: {file_path}")
            return 2
        result = sender.send_file(
            file_path=file_path,
            destination_host=args.dest_host,
            destination_port=args.dest_port,
            stop_requested=lambda: should_stop,
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


@dataclasses.dataclass(slots=True)
class _RevisitEntry:
    source_file: Path
    destination_host: str
    remote_name: str
    transfer_id_hex: str
    attempts: int = 0


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

    feedback_forced_on = args.feedback is True
    feedback_forced_off = args.feedback is False
    sender = SpaceSyncSender(
        config=SenderConfig(
            chunk_size=args.chunk_size,
            manifest_repeats=args.manifest_repeats,
            inter_packet_delay_s=args.inter_packet_delay_s,
            enable_feedback=feedback_forced_on,
            auto_feedback_discovery=not (feedback_forced_on or feedback_forced_off),
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
            periodic_metadata_interval_s=max(0.0, args.periodic_metadata_interval_s),
            periodic_metadata_every_n_chunks=max(0, args.periodic_metadata_every_n_chunks),
            revisit_incomplete_passes=max(0, args.revisit_incomplete_passes),
            revisit_max_rounds_per_pass=max(0, args.revisit_max_rounds_per_pass),
            primary_feedback_max_rounds=max(0, args.primary_feedback_max_rounds),
            primary_feedback_max_seconds=max(0.0, args.primary_feedback_max_seconds),
        )
    )

    failed = 0
    sent_count = 0
    skipped_count = 0
    dry_run_count = 0
    should_query_destination = bool(args.skip_unchanged)
    def _open_loop_mode_active() -> bool:
        if feedback_forced_off:
            return True
        if feedback_forced_on:
            return False
        return not bool(getattr(sender, "_auto_feedback_active", False))
    if args.open_loop_max_rounds < 0:
        print("sync error: --open-loop-max-rounds must be >= 0")
        return 2
    if args.revisit_incomplete_passes < 0:
        print("sync error: --revisit-incomplete-passes must be >= 0")
        return 2
    if args.revisit_max_rounds_per_pass < 0:
        print("sync error: --revisit-max-rounds-per-pass must be >= 0")
        return 2
    if args.primary_feedback_max_rounds < 0:
        print("sync error: --primary-feedback-max-rounds must be >= 0")
        return 2
    if args.primary_feedback_max_seconds < 0:
        print("sync error: --primary-feedback-max-seconds must be >= 0")
        return 2
    open_loop_state = _load_open_loop_state(args.state_file)
    round_index = 0
    should_stop = False

    def _handle_signal(_signum: int, _frame: object) -> None:
        nonlocal should_stop
        should_stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collect_item_results = (
        (not _open_loop_mode_active())
        or args.dry_run
        or args.open_loop_max_rounds > 0
    )
    item_results: list[dict[str, object]] | None = [] if collect_item_results else None
    total_items = sum(len(items) for _, items in sync_plans)
    revisit_enabled = (
        (not feedback_forced_off)
        and not args.dry_run
        and args.revisit_incomplete_passes > 0
    )
    revisit_queue: collections.deque[_RevisitEntry] = collections.deque()
    revisit_active_keys: set[tuple[str, str]] = set()

    def _enqueue_revisit(
        *,
        source_file: Path,
        destination_host: str,
        remote_name: str,
        transfer_id_hex: str,
    ) -> None:
        revisit_key = (destination_host, remote_name)
        if revisit_key in revisit_active_keys:
            return
        revisit_queue.append(
            _RevisitEntry(
                source_file=source_file,
                destination_host=destination_host,
                remote_name=remote_name,
                transfer_id_hex=transfer_id_hex,
            )
        )
        revisit_active_keys.add(revisit_key)

    def _run_revisit_attempts(max_attempts: int) -> tuple[int, int]:
        nonlocal should_stop, sent_count, failed
        completed_transfers = 0
        retired_incomplete_transfers = 0
        attempts_remaining = max(0, max_attempts)
        while revisit_queue and attempts_remaining > 0 and not should_stop:
            attempts_remaining -= 1
            entry = revisit_queue.popleft()
            destination_host = entry.destination_host
            remote_name = entry.remote_name
            revisit_key = (destination_host, remote_name)
            transfer_id_hex = entry.transfer_id_hex
            attempts = entry.attempts + 1
            try:
                transfer_id = bytes.fromhex(transfer_id_hex)
            except ValueError:
                transfer_id = b""
            if len(transfer_id) != 16:
                failed += 1
                retired_incomplete_transfers += 1
                revisit_active_keys.discard(revisit_key)
                continue
            source_file = entry.source_file
            result = sender.send_file(
                file_path=source_file,
                destination_host=destination_host,
                destination_port=args.dest_port,
                remote_name=remote_name,
                stop_requested=lambda: should_stop,
                transfer_id=transfer_id,
                send_initial_data=False,
                max_repair_rounds_override=args.revisit_max_rounds_per_pass,
                max_feedback_seconds_override=0.0,
                max_feedback_total_rounds_override=0,
            )
            status = "revisit-sent" if result.completed else "revisit-incomplete"
            item_result = {
                "status": status,
                "source": str(source_file),
                "destination": f"{destination_host}:{remote_name}",
                "transfer_id": result.transfer_id_hex,
                "chunks": result.total_chunks,
                "repaired": result.repaired_chunks,
                "rounds": result.repair_rounds,
                "completed": result.completed,
                "revisit_attempt": attempts,
            }
            if item_results is not None:
                item_results.append(item_result)
            if args.verbose and not args.json_output:
                print(
                    f"[{status}] {source_file} -> {destination_host}:{remote_name} "
                    f"completed={result.completed} attempt={attempts}"
                )
            if result.completed:
                sent_count += 1
                completed_transfers += 1
                revisit_active_keys.discard(revisit_key)
                continue
            if attempts >= args.revisit_incomplete_passes:
                failed += 1
                retired_incomplete_transfers += 1
                revisit_active_keys.discard(revisit_key)
                continue
            entry.attempts = attempts
            revisit_queue.append(entry)
        return completed_transfers, retired_incomplete_transfers

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
                if _open_loop_mode_active()
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
                    result = sender.send_file(
                        file_path=source_file,
                        destination_host=destination_host,
                        destination_port=args.dest_port,
                        remote_name=remote_name,
                        stop_requested=lambda: should_stop,
                        max_feedback_seconds_override=args.primary_feedback_max_seconds,
                        max_feedback_total_rounds_override=args.primary_feedback_max_rounds,
                    )
                    status = "sent" if result.completed else "incomplete"
                    if not result.completed:
                        if revisit_enabled:
                            _enqueue_revisit(
                                source_file=source_file,
                                destination_host=destination_host,
                                remote_name=remote_name,
                                transfer_id_hex=result.transfer_id_hex,
                            )
                        else:
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
                    if _open_loop_mode_active():
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
                if revisit_enabled and not should_stop:
                    _run_revisit_attempts(1)

        if args.dry_run:
            break
        if should_stop:
            break
        if not _open_loop_mode_active():
            break
        if args.open_loop_max_rounds > 0 and round_index >= args.open_loop_max_rounds:
            break

    if revisit_enabled and not should_stop:
        while revisit_queue:
            queue_size = len(revisit_queue)
            completed_now, retired_now = _run_revisit_attempts(queue_size)
            if completed_now == 0 and retired_now == 0:
                break
    if revisit_queue:
        failed += len(revisit_queue)
        revisit_queue.clear()

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
    if argv and argv[0] == "sync":
        print("sync error: 'ssync sync' is deprecated; use 'ssync <sources> <destination>'")
        return 2
    subcommands = {"receive", "server", "ssyncd", "send", "monitor"}
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


def ssyncd_main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = _build_ssyncd_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    return _run_server(args)


if __name__ == "__main__":
    raise SystemExit(main())

