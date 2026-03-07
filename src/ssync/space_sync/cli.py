from __future__ import annotations

import argparse
import fnmatch
import json
import signal
import sys
import time
from pathlib import Path, PurePosixPath

from .receiver import SpaceSyncReceiver
from .sender import SpaceSyncSender
from .types import ReceiverConfig, RemoteFileInfo, SenderConfig


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Space Sync UDP file transport prototype")
    subparsers = parser.add_subparsers(dest="command", required=True)

    recv = subparsers.add_parser("receive", help="Run a Space Sync receiver")
    recv.add_argument("--bind-host", default="127.0.0.1")
    recv.add_argument("--bind-port", type=int, default=9000)
    recv.add_argument("--output-dir", type=Path, default=Path("./received"))
    recv.add_argument("--feedback", action="store_true", help="Enable repair feedback")
    recv.add_argument(
        "--status-repeat",
        type=int,
        default=1,
        help="How many times status is repeated when feedback is enabled",
    )

    server = subparsers.add_parser(
        "server",
        help="Run a destination server for rsync-like ssync sync operations",
    )
    server.add_argument("--bind-host", default="0.0.0.0")
    server.add_argument("--bind-port", type=int, default=9000)
    server.add_argument(
        "--root-dir",
        type=Path,
        default=Path("./received"),
        help="Root directory where incoming files are written",
    )
    server.add_argument(
        "--feedback",
        action="store_true",
        default=True,
        help="Enable repair feedback (default: enabled)",
    )
    server.add_argument(
        "--no-feedback",
        action="store_false",
        dest="feedback",
        help="Disable feedback for open-loop only operation",
    )
    server.add_argument(
        "--status-repeat",
        type=int,
        default=1,
        help="How many times status is repeated when feedback is enabled",
    )

    send = subparsers.add_parser("send", help="Send a file over Space Sync")
    send.add_argument("file", type=Path)
    send.add_argument("--dest-host", default="127.0.0.1")
    send.add_argument("--dest-port", type=int, default=9000)
    send.add_argument("--chunk-size", type=int, default=1024)
    send.add_argument("--manifest-repeats", type=int, default=3)
    send.add_argument("--feedback", action="store_true", help="Enable repair flow")
    send.add_argument("--feedback-wait-s", type=float, default=2.0)
    send.add_argument("--max-repair-rounds", type=int, default=2)
    send.add_argument("--max-feedback-idle-timeouts", type=int, default=2)
    send.add_argument(
        "--drop-every-nth-data",
        type=int,
        default=0,
        help="Test helper: drop every nth data frame",
    )
    send.add_argument("--inter-packet-delay-s", type=float, default=0.0)
    send.add_argument("--json", action="store_true", dest="json_output")

    sync = subparsers.add_parser(
        "sync",
        help="Compatibility alias for rsync-like sync behavior",
    )
    _add_sync_args(sync)
    return parser


def _build_rsync_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Space Sync rsync-like file synchronization")
    _add_sync_args(parser)
    return parser


def _add_sync_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("source", type=Path, help="Source file or directory")
    parser.add_argument("destination", help="Destination in host:path form")
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
    parser.add_argument("--max-repair-rounds", type=int, default=2)
    parser.add_argument("--max-feedback-idle-timeouts", type=int, default=2)
    parser.add_argument(
        "--drop-every-nth-data",
        type=int,
        default=0,
        help="Test helper: drop every nth data frame",
    )
    parser.add_argument("--inter-packet-delay-s", type=float, default=0.0)
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
    status_repeat: int,
    banner: str,
) -> int:
    receiver = SpaceSyncReceiver(
        bind_host=bind_host,
        bind_port=bind_port,
        config=ReceiverConfig(
            output_dir=output_dir,
            enable_feedback=feedback,
            status_repeat=max(1, status_repeat),
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
        status_repeat=args.status_repeat,
        banner=f"Space Sync receiver listening on {args.bind_host}:{args.bind_port}",
    )


def _run_server(args: argparse.Namespace) -> int:
    return _run_receiver_common(
        bind_host=args.bind_host,
        bind_port=args.bind_port,
        output_dir=args.root_dir,
        feedback=args.feedback,
        status_repeat=args.status_repeat,
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
        )
    )
    result = sender.send_file(
        file_path=args.file,
        destination_host=args.dest_host,
        destination_port=args.dest_port,
    )
    if args.json_output:
        print(
            json.dumps(
                {
                    "transfer_id": result.transfer_id_hex,
                    "chunks": result.total_chunks,
                    "repaired": result.repaired_chunks,
                    "rounds": result.repair_rounds,
                    "completed": result.completed,
                }
            )
        )
    else:
        print(
            "transfer_id="
            f"{result.transfer_id_hex} chunks={result.total_chunks} "
            f"repaired={result.repaired_chunks} rounds={result.repair_rounds} "
            f"completed={result.completed}"
        )
    return 0 if result.completed else 1


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
) -> list[tuple[Path, str]]:
    source = source.resolve()
    remote_root_path = PurePosixPath(remote_root)
    if source.is_file():
        if remote_root.endswith("/"):
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
            remote_name = str(remote_root_path / relative)
            items.append((file_path, remote_name))
        return items
    raise ValueError(f"source not found: {source}")


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
    try:
        destination_host, remote_root = _parse_destination(args.destination)
        items = _collect_sync_items(
            args.source,
            remote_root,
            recursive=args.recursive,
            includes=args.include,
            excludes=args.exclude,
        )
    except ValueError as exc:
        print(f"sync error: {exc}")
        return 2
    if args.delete:
        print("sync error: --delete is not implemented yet")
        return 2
    if args.checksum and not args.skip_unchanged:
        print("sync error: --checksum requires --skip-unchanged")
        return 2
    if not items:
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
        )
    )

    failed = 0
    sent_count = 0
    skipped_count = 0
    dry_run_count = 0
    should_query_destination = bool(args.skip_unchanged)
    open_loop_mode = not args.feedback
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

    item_results: list[dict[str, object]] = []
    while True:
        round_index += 1
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
                            "files": len(items),
                            "incomplete": failed,
                            "sent": sent_count,
                            "skipped": skipped_count,
                            "would_send": dry_run_count,
                            "success": False,
                        },
                        "results": item_results,
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
                        "files": len(items),
                        "incomplete": 0,
                        "sent": sent_count,
                        "skipped": skipped_count,
                        "would_send": dry_run_count,
                        "success": True,
                    },
                    "results": item_results,
                }
            )
        )
    else:
        print(
            "sync complete: "
            f"files={len(items)} sent={sent_count} skipped={skipped_count} "
            f"would_send={dry_run_count}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    subcommands = {"receive", "server", "send", "sync"}
    if argv and argv[0] in subcommands:
        parser = _build_parser()
        args = parser.parse_args(argv)
    else:
        parser = _build_rsync_parser()
        args = parser.parse_args(argv)
        args.command = "sync"
    if args.command == "receive":
        return _run_receiver(args)
    if args.command == "server":
        return _run_server(args)
    if args.command == "send":
        return _run_sender(args)
    if args.command == "sync":
        return _run_sync(args)
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

