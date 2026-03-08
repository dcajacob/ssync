#!/usr/bin/env python3
# pyright: reportMissingImports=false, reportMissingModuleSource=false
# ruff: noqa: I001
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ssync.space_sync.receiver import SpaceSyncReceiver
from ssync.space_sync.sender import SpaceSyncSender
from ssync.space_sync.types import ReceiverConfig, SendResult, SenderConfig


@dataclass(slots=True)
class TrialResult:
    rate_bps: int
    trial_index: int
    success: bool
    duration_s: float
    achieved_bps: float
    send_completed: bool
    output_present: bool
    hash_match: bool
    repaired_chunks: int
    repair_rounds: int
    transfer_id_hex: str
    note: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark loopback transfer rate by sweeping sender max-data-rate-bps "
            "and reporting the highest stable value"
        )
    )
    parser.add_argument(
        "--rates-bps",
        help="Comma-separated exact rates to test in bits/sec instead of auto search",
    )
    parser.add_argument(
        "--start-bps",
        type=int,
        default=1_000_000,
        help="Initial auto-search rate in bits/sec",
    )
    parser.add_argument(
        "--max-bps",
        type=int,
        default=100_000_000,
        help="Maximum auto-search rate in bits/sec",
    )
    parser.add_argument(
        "--growth-factor",
        type=float,
        default=2.0,
        help="Auto-search multiplier while rates keep passing",
    )
    parser.add_argument(
        "--refine-steps",
        type=int,
        default=6,
        help="Binary-search refinement steps after the first failure",
    )
    parser.add_argument(
        "--round-bps",
        type=int,
        default=250_000,
        help="Round auto-generated candidate rates to this many bits/sec",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=1,
        help="Trials per tested rate; all must pass for the rate to count as stable",
    )
    parser.add_argument(
        "--file-size-mib",
        type=int,
        default=64,
        help="Benchmark payload size in MiB",
    )
    parser.add_argument("--chunk-size", type=int, default=1024)
    parser.add_argument("--manifest-repeats", type=int, default=3)
    parser.add_argument("--inter-packet-delay-s", type=float, default=0.0002)
    parser.add_argument(
        "--feedback",
        action="store_true",
        default=True,
        help="Enable receiver feedback and repair flow (default: enabled)",
    )
    parser.add_argument(
        "--no-feedback",
        action="store_false",
        dest="feedback",
        help="Disable receiver feedback and benchmark open-loop only",
    )
    parser.add_argument("--feedback-wait-s", type=float, default=2.0)
    parser.add_argument("--max-repair-rounds", type=int, default=0)
    parser.add_argument("--max-feedback-idle-timeouts", type=int, default=60)
    parser.add_argument("--midstream-repair-max-rounds-per-poll", type=int, default=1)
    parser.add_argument("--midstream-repair-max-chunks-per-poll", type=int, default=256)
    parser.add_argument("--repair-duplicate-suppression-s", type=float, default=0.2)
    parser.add_argument("--drop-every-nth-data", type=int, default=0)
    parser.add_argument("--periodic-repair-request-s", type=float, default=0.5)
    parser.add_argument("--periodic-repair-min-seen-chunks", type=int, default=32)
    parser.add_argument("--max-repair-chunks-per-request", type=int, default=256)
    parser.add_argument("--repair-request-cooldown-s", type=float, default=0.2)
    parser.add_argument("--repair-request-inflight-timeout-s", type=float, default=1.5)
    parser.add_argument("--transfer-inactivity-timeout-s", type=float, default=10.0)
    parser.add_argument("--socket-rcvbuf-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--journal-flush-interval-s", type=float, default=1.0)
    parser.add_argument("--status-repeat", type=int, default=5)
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable the live tqdm progress bar",
    )
    parser.add_argument(
        "--output-json",
        action="store_true",
        help="Emit machine-readable JSON after the text summary",
    )
    return parser.parse_args()


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_payload(path: Path, size_bytes: int) -> None:
    block = hashlib.sha256(b"ssync-benchmark-payload").digest() * 32_768
    with path.open("wb") as stream:
        remaining = size_bytes
        while remaining > 0:
            piece = block[: min(len(block), remaining)]
            stream.write(piece)
            remaining -= len(piece)


def _wait_for_file(path: Path, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.05)
    return False


def _write_line(args: argparse.Namespace, message: str) -> None:
    if args.no_progress:
        print(message)
        return
    tqdm.write(message)


def _format_bps(rate_bps: int) -> str:
    if rate_bps >= 1_000_000_000:
        return f"{rate_bps / 1_000_000_000:.2f} Gbps"
    if rate_bps >= 1_000_000:
        return f"{rate_bps / 1_000_000:.2f} Mbps"
    if rate_bps >= 1_000:
        return f"{rate_bps / 1_000:.2f} Kbps"
    return f"{rate_bps} bps"


def _round_rate(candidate_bps: int, round_bps: int) -> int:
    if round_bps <= 0:
        return candidate_bps
    rounded = int(round(candidate_bps / round_bps) * round_bps)
    return max(round_bps, rounded)


def _expected_timeout_s(file_size_bytes: int, rate_bps: int) -> float:
    if rate_bps <= 0:
        return 30.0
    expected = (file_size_bytes * 8) / float(rate_bps)
    return max(20.0, expected * 3.0 + 15.0)


def _read_received_bytes(receiver_dir: Path) -> tuple[int | None, int | None, str | None]:
    final_paths = list(receiver_dir.glob("*"))
    if any(path.is_file() and not path.name.startswith(".") for path in final_paths):
        target = next(
            path
            for path in final_paths
            if path.is_file() and not path.name.startswith(".")
        )
        size = target.stat().st_size
        return size, size, None

    journal_path = receiver_dir / ".ssync-journal.json"
    if not journal_path.exists():
        return None, None, None
    try:
        data = json.loads(journal_path.read_text())
        transfers = data.get("transfers", [])
        if not transfers:
            return None, None, None
        transfer = transfers[0]
        manifest = transfer["manifest"]
        chunk_size = int(manifest["chunk_size"])
        file_size = int(manifest["file_size"])
        received_ranges = transfer.get("received_ranges", [])
        received_bytes = 0
        for start, end in received_ranges:
            start_offset = int(start) * chunk_size
            end_offset = min(int(end) * chunk_size, file_size)
            if end_offset > start_offset:
                received_bytes += end_offset - start_offset
        transfer_id_hex = str(transfer.get("transfer_id_hex", ""))
        return received_bytes, file_size, transfer_id_hex or None
    except (OSError, ValueError, TypeError, KeyError):
        return None, None, None


def _run_sender_in_thread(
    sender: SpaceSyncSender,
    source: Path,
    port: int,
    result_box: dict[str, SendResult],
    error_box: dict[str, BaseException],
) -> None:
    try:
        result_box["send_result"] = sender.send_file(source, "127.0.0.1", port)
    except BaseException as exc:  # pragma: no cover - surfaces back to caller
        error_box["exception"] = exc


def _send_with_progress(
    *,
    sender: SpaceSyncSender,
    source: Path,
    receiver_dir: Path,
    port: int,
    rate_bps: int,
    trial_index: int,
    args: argparse.Namespace,
) -> SendResult:
    result_box: dict[str, SendResult] = {}
    error_box: dict[str, BaseException] = {}
    send_thread = threading.Thread(
        target=_run_sender_in_thread,
        args=(sender, source, port, result_box, error_box),
        daemon=True,
    )
    total_bytes = source.stat().st_size
    progress = tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        desc=f"{_format_bps(rate_bps)} trial {trial_index}",
        leave=False,
        disable=args.no_progress,
    )
    send_thread.start()
    try:
        while send_thread.is_alive():
            received_bytes, journal_total_bytes, transfer_id_hex = _read_received_bytes(
                receiver_dir
            )
            if journal_total_bytes is not None and progress.total != journal_total_bytes:
                progress.total = journal_total_bytes
            if received_bytes is not None and received_bytes > progress.n:
                progress.update(received_bytes - progress.n)
            if transfer_id_hex:
                progress.set_postfix_str(f"transfer={transfer_id_hex[:8]}")
            time.sleep(0.2)
        send_thread.join()
        received_bytes, journal_total_bytes, transfer_id_hex = _read_received_bytes(receiver_dir)
        if journal_total_bytes is not None and progress.total != journal_total_bytes:
            progress.total = journal_total_bytes
        if received_bytes is not None and received_bytes > progress.n:
            progress.update(received_bytes - progress.n)
        if transfer_id_hex:
            progress.set_postfix_str(f"transfer={transfer_id_hex[:8]}")
        if "exception" in error_box:
            raise error_box["exception"]
        return result_box["send_result"]
    finally:
        progress.close()


def _run_trial(
    *,
    work_dir: Path,
    source: Path,
    source_hash: str,
    rate_bps: int,
    trial_index: int,
    args: argparse.Namespace,
) -> TrialResult:
    receiver_dir = work_dir / f"rx-{rate_bps}-{trial_index}"
    receiver_dir.mkdir(parents=True, exist_ok=True)
    port = _free_udp_port()
    receiver = SpaceSyncReceiver(
        bind_host="127.0.0.1",
        bind_port=port,
        config=ReceiverConfig(
            output_dir=receiver_dir,
            enable_feedback=args.feedback,
            status_repeat=args.status_repeat,
            periodic_repair_request_s=args.periodic_repair_request_s,
            periodic_repair_min_seen_chunks=args.periodic_repair_min_seen_chunks,
            max_repair_chunks_per_request=args.max_repair_chunks_per_request,
            transfer_inactivity_timeout_s=args.transfer_inactivity_timeout_s,
            socket_rcvbuf_bytes=args.socket_rcvbuf_bytes,
            journal_flush_interval_s=args.journal_flush_interval_s,
            repair_request_cooldown_s=args.repair_request_cooldown_s,
            repair_request_inflight_timeout_s=args.repair_request_inflight_timeout_s,
        ),
    )
    receiver.start()
    time.sleep(0.15)
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
            max_data_rate_bps=rate_bps,
            midstream_repair_max_rounds_per_poll=args.midstream_repair_max_rounds_per_poll,
            midstream_repair_max_chunks_per_poll=args.midstream_repair_max_chunks_per_poll,
            repair_duplicate_suppression_s=args.repair_duplicate_suppression_s,
        )
    )
    started = time.monotonic()
    try:
        send_result = _send_with_progress(
            sender=sender,
            source=source,
            receiver_dir=receiver_dir,
            port=port,
            rate_bps=rate_bps,
            trial_index=trial_index,
            args=args,
        )
        duration_s = time.monotonic() - started
        target = receiver_dir / source.name
        output_present = _wait_for_file(
            target,
            timeout_s=_expected_timeout_s(source.stat().st_size, rate_bps),
        )
        hash_match = output_present and _sha256(target) == source_hash
        success = bool(send_result.completed and output_present and hash_match)
        note = "ok"
        if not send_result.completed:
            note = "sender_incomplete"
        elif not output_present:
            note = "receiver_missing_output"
        elif not hash_match:
            note = "hash_mismatch"
        achieved_bps = 0.0
        if duration_s > 0:
            achieved_bps = (source.stat().st_size * 8) / duration_s
        return TrialResult(
            rate_bps=rate_bps,
            trial_index=trial_index,
            success=success,
            duration_s=duration_s,
            achieved_bps=achieved_bps,
            send_completed=send_result.completed,
            output_present=output_present,
            hash_match=hash_match,
            repaired_chunks=send_result.repaired_chunks,
            repair_rounds=send_result.repair_rounds,
            transfer_id_hex=send_result.transfer_id_hex,
            note=note,
        )
    finally:
        receiver.stop()


def _run_rate(
    *,
    work_dir: Path,
    source: Path,
    source_hash: str,
    rate_bps: int,
    args: argparse.Namespace,
) -> list[TrialResult]:
    results: list[TrialResult] = []
    _write_line(args, f"Testing {rate_bps} bps ({_format_bps(rate_bps)})...")
    for trial_index in range(1, args.trials + 1):
        result = _run_trial(
            work_dir=work_dir,
            source=source,
            source_hash=source_hash,
            rate_bps=rate_bps,
            trial_index=trial_index,
            args=args,
        )
        results.append(result)
        _write_line(
            args,
            "  "
            f"trial={trial_index} success={result.success} completed={result.send_completed} "
            f"duration={result.duration_s:.2f}s achieved={_format_bps(int(result.achieved_bps))} "
            f"repaired={result.repaired_chunks} rounds={result.repair_rounds} note={result.note}",
        )
        if not result.success:
            break
    return results


def _auto_rates(args: argparse.Namespace) -> list[int]:
    if args.rates_bps:
        values = [int(part.strip()) for part in args.rates_bps.split(",") if part.strip()]
        return sorted({value for value in values if value > 0})

    tested: list[int] = []
    current = _round_rate(max(1, args.start_bps), args.round_bps)
    while current <= args.max_bps:
        if current not in tested:
            tested.append(current)
        next_rate = _round_rate(int(current * args.growth_factor), args.round_bps)
        if next_rate <= current:
            next_rate = current + max(1, args.round_bps)
        current = next_rate
    return tested


def _search_rates(
    *,
    work_dir: Path,
    source: Path,
    source_hash: str,
    args: argparse.Namespace,
) -> tuple[list[TrialResult], int | None]:
    all_results: list[TrialResult] = []
    exact_rates = _auto_rates(args)
    tried_rates: dict[int, bool] = {}
    last_good: int | None = None
    first_bad: int | None = None

    for rate_bps in exact_rates:
        rate_results = _run_rate(
            work_dir=work_dir,
            source=source,
            source_hash=source_hash,
            rate_bps=rate_bps,
            args=args,
        )
        all_results.extend(rate_results)
        rate_ok = all(result.success for result in rate_results)
        tried_rates[rate_bps] = rate_ok
        if rate_ok:
            last_good = rate_bps
            continue
        first_bad = rate_bps
        break

    if args.rates_bps or last_good is None or first_bad is None:
        return all_results, last_good

    low = last_good
    high = first_bad
    for _ in range(max(0, args.refine_steps)):
        candidate = _round_rate((low + high) // 2, args.round_bps)
        if candidate <= low or candidate >= high or candidate in tried_rates:
            break
        rate_results = _run_rate(
            work_dir=work_dir,
            source=source,
            source_hash=source_hash,
            rate_bps=candidate,
            args=args,
        )
        all_results.extend(rate_results)
        rate_ok = all(result.success for result in rate_results)
        tried_rates[candidate] = rate_ok
        if rate_ok:
            low = candidate
        else:
            high = candidate
    return all_results, low


def _successful_results(results: list[TrialResult]) -> list[TrialResult]:
    return [result for result in results if result.success]


def _best_achieved_result(results: list[TrialResult]) -> TrialResult | None:
    successful = _successful_results(results)
    if not successful:
        return None
    return max(successful, key=lambda result: result.achieved_bps)


def _detect_saturation_note(results: list[TrialResult]) -> str | None:
    successful = sorted(_successful_results(results), key=lambda result: result.rate_bps)
    if len(successful) < 2:
        return None

    highest = successful[-1]
    previous = successful[-2]
    if previous.achieved_bps <= 0:
        return None

    configured_ratio = highest.rate_bps / previous.rate_bps
    achieved_ratio = highest.achieved_bps / previous.achieved_bps
    utilization = highest.achieved_bps / highest.rate_bps if highest.rate_bps > 0 else 0.0

    if configured_ratio >= 1.5 and achieved_ratio <= 1.1 and utilization < 0.9:
        return (
            "Saturation detected: increasing the configured cap from "
            f"{_format_bps(previous.rate_bps)} to {_format_bps(highest.rate_bps)} only raised "
            f"achieved throughput from {_format_bps(int(previous.achieved_bps))} to "
            f"{_format_bps(int(highest.achieved_bps))}."
        )
    return None


def main() -> int:
    args = _parse_args()
    file_size_bytes = args.file_size_mib * 1024 * 1024

    with tempfile.TemporaryDirectory(prefix="ssync-benchmark-") as temp_root:
        work_dir = Path(temp_root)
        source = work_dir / "benchmark.bin"
        _write_line(args, f"Preparing {args.file_size_mib} MiB benchmark payload...")
        _write_payload(source, file_size_bytes)
        source_hash = _sha256(source)

        all_results, best_rate = _search_rates(
            work_dir=work_dir,
            source=source,
            source_hash=source_hash,
            args=args,
        )

    print()
    print("Space Sync loopback benchmark summary")
    print("=" * 36)
    if best_rate is None:
        print("No stable rate found in the tested range.")
    else:
        print(f"Best stable rate: {best_rate} bps ({_format_bps(best_rate)})")
    best_achieved = _best_achieved_result(all_results)
    if best_achieved is not None:
        print(
            "Best achieved throughput: "
            f"{_format_bps(int(best_achieved.achieved_bps))} "
            f"at configured {_format_bps(best_achieved.rate_bps)}"
        )
    saturation_note = _detect_saturation_note(all_results)
    if saturation_note is not None:
        print(saturation_note)

    if all_results:
        print()
        print("Per-trial results:")
        for result in all_results:
            achieved = _format_bps(int(result.achieved_bps))
            print(
                "  "
                f"rate={result.rate_bps} trial={result.trial_index} success={result.success} "
                f"completed={result.send_completed} hash_match={result.hash_match} "
                f"duration={result.duration_s:.2f}s achieved={achieved} "
                f"repaired={result.repaired_chunks} "
                f"rounds={result.repair_rounds} note={result.note}"
            )

    if args.output_json:
        payload = {
            "best_stable_rate_bps": best_rate,
            "best_achieved_throughput_bps": (
                int(best_achieved.achieved_bps) if best_achieved is not None else None
            ),
            "best_achieved_configured_rate_bps": (
                best_achieved.rate_bps if best_achieved is not None else None
            ),
            "saturation_note": saturation_note,
            "file_size_mib": args.file_size_mib,
            "feedback": args.feedback,
            "results": [asdict(result) for result in all_results],
        }
        print()
        print(json.dumps(payload, indent=2))

    return 0 if best_rate is not None else 1


if __name__ == "__main__":
    raise SystemExit(main())
