from __future__ import annotations

import argparse
import signal
import time
from pathlib import Path
from pathlib import PurePosixPath

from .receiver import SpaceSyncReceiver
from .sender import SpaceSyncSender
from .types import ReceiverConfig, SenderConfig


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
    send.add_argument(
        "--drop-every-nth-data",
        type=int,
        default=0,
        help="Test helper: drop every nth data frame",
    )
    send.add_argument("--inter-packet-delay-s", type=float, default=0.0)

    sync = subparsers.add_parser(
        "sync",
        help="Rsync-like sync: send source to destination host:path",
    )
    sync.add_argument("source", type=Path, help="Source file or directory")
    sync.add_argument(
        "destination",
        help="Destination in host:path form (for example 127.0.0.1:data/drop)",
    )
    sync.add_argument("--dest-port", type=int, default=9000)
    sync.add_argument("--chunk-size", type=int, default=1024)
    sync.add_argument("--manifest-repeats", type=int, default=3)
    sync.add_argument(
        "--feedback",
        action="store_true",
        default=True,
        help="Enable repair flow (default: enabled)",
    )
    sync.add_argument(
        "--no-feedback",
        action="store_false",
        dest="feedback",
        help="Disable feedback/repair flow",
    )
    sync.add_argument("--feedback-wait-s", type=float, default=2.0)
    sync.add_argument("--max-repair-rounds", type=int, default=2)
    sync.add_argument(
        "--drop-every-nth-data",
        type=int,
        default=0,
        help="Test helper: drop every nth data frame",
    )
    sync.add_argument("--inter-packet-delay-s", type=float, default=0.0)
    return parser


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
            drop_every_nth_data=args.drop_every_nth_data,
        )
    )
    result = sender.send_file(
        file_path=args.file,
        destination_host=args.dest_host,
        destination_port=args.dest_port,
    )
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
    if remote_path.startswith("/"):
        raise ValueError("destination path must be relative to the server root")
    return host, remote_path


def _collect_sync_items(source: Path, remote_root: str) -> list[tuple[Path, str]]:
    source = source.resolve()
    remote_root_path = PurePosixPath(remote_root)
    if source.is_file():
        if remote_root.endswith("/"):
            remote_name = str(remote_root_path / source.name)
        else:
            remote_name = str(remote_root_path)
        return [(source, remote_name)]
    if source.is_dir():
        files = sorted(path for path in source.rglob("*") if path.is_file())
        items: list[tuple[Path, str]] = []
        for file_path in files:
            relative = file_path.relative_to(source).as_posix()
            remote_name = str(remote_root_path / relative)
            items.append((file_path, remote_name))
        return items
    raise ValueError(f"source not found: {source}")


def _run_sync(args: argparse.Namespace) -> int:
    try:
        destination_host, remote_root = _parse_destination(args.destination)
        items = _collect_sync_items(args.source, remote_root)
    except ValueError as exc:
        print(f"sync error: {exc}")
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
            drop_every_nth_data=args.drop_every_nth_data,
        )
    )

    failed = 0
    for source_file, remote_name in items:
        result = sender.send_file(
            file_path=source_file,
            destination_host=destination_host,
            destination_port=args.dest_port,
            remote_name=remote_name,
        )
        status = "ok" if result.completed else "incomplete"
        if not result.completed:
            failed += 1
        print(
            f"[{status}] {source_file} -> {destination_host}:{remote_name} "
            f"transfer_id={result.transfer_id_hex} chunks={result.total_chunks} "
            f"repaired={result.repaired_chunks} rounds={result.repair_rounds}"
        )
    if failed:
        print(f"sync completed with {failed} incomplete transfer(s)")
        return 1
    print(f"sync complete: {len(items)} file(s) transferred")
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
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

