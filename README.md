# Space Sync (`ssync`)

Space Sync is an initial UDP transport protocol prototype for mission-specific satellite
communications, focused on reliable file delivery over asymmetric and intermittent links.

This repository includes:

- binary framing and protocol codecs
- sender and receiver runtime logic
- open-loop transfer mode (no return path required)
- feedback-assisted repair mode (sparse missing-range requests)
- tests for core behavior and local end-to-end transfer

## Python and environment

- Python `>=3.13`
- dependency-light runtime (uses `rich` for monitor TUI)
- tooling is `uv`-friendly

## Quick start

Install editable package and test dependencies:

```bash
uv sync --dev
```

Run receiver (no feedback):

```bash
uv run ssync receive --bind-host 127.0.0.1 --bind-port 9000 --output-dir ./received
```

Send a file:

```bash
uv run ssync send ./example.bin --dest-host 127.0.0.1 --dest-port 9000
```

Run feedback/repair mode:

```bash
uv run ssync receive --bind-port 9000 --output-dir ./received --feedback
uv run ssync send ./example.bin --dest-port 9000 --feedback
```

Rsync-like workflow (destination runs a server):

```bash
# destination host
uv run ssync server --bind-port 9000 --root-dir ./received
# equivalent daemon alias
uv run ssyncd --bind-port 9000 --root-dir ./received
# optional: keep hidden .part files after successful completion for debugging
uv run ssyncd --bind-port 9000 --root-dir ./received --keep-part-files-on-complete

# source host
uv run ssync ./example.bin 127.0.0.1:incoming/example.bin --dest-port 9000
```

Sync a directory tree to a destination root path:

```bash
uv run ssync -r ./payloads 127.0.0.1:missions/pass-001/ --dest-port 9000
```

The top-level `ssync SRC DEST` workflow enables auto feedback discovery by default:
it starts open-loop, promotes to feedback when uplink packets/beacons are observed,
and can fall back to open-loop if uplink goes idle. Use `--feedback` or
`--no-feedback` to force either mode.

Rsync-style convenience options:

```bash
# dry run
uv run ssync -n -r ./payloads 127.0.0.1:missions/pass-001/

# include/exclude filters
uv run ssync -r --include "*.txt" --exclude "tmp/*" ./payloads 127.0.0.1:missions/pass-001/

# checksum-based unchanged detection
uv run ssync -r --skip-unchanged --checksum ./payloads 127.0.0.1:missions/pass-001/
```

`uv run ssync sync SRC DEST` is deprecated; use `uv run ssync SRC DEST`.

By default, sync does not pre-query the destination; it streams files immediately.
Use `--skip-unchanged` (optionally with `--checksum`) when you want pre-transfer
unchanged detection.

Open-loop behavior (`--no-feedback`) is round-based and continuous by default:
once the file set is finished, `ssync` starts another round. It keeps a persistent
send-state file (`.ssync-open-loop-state.json`) with retransmission counts and
orders each round so files with the lowest retransmission count are sent first.

```bash
# run open-loop continuously
uv run ssync -r --no-feedback ./payloads 127.0.0.1:missions/pass-001/

# run exactly two open-loop rounds
uv run ssync -r --no-feedback --open-loop-max-rounds 2 ./payloads 127.0.0.1:missions/pass-001/
```

Machine-readable output for automation:

```bash
uv run ssync send ./example.bin --dest-port 9000 --json
uv run ssync -r ./payloads 127.0.0.1:missions/pass-001/ --dest-port 9000 --json
```

Feedback mode timing controls:

```bash
uv run ssync send ./example.bin --dest-port 9000 --feedback \
  --feedback-wait-s 3.0 --max-feedback-idle-timeouts 10 --max-repair-rounds 32 \
  --repair-worker-max-chunks-per-burst 256 \
  --initial-pass-repair-max-chunks-per-burst 16
```

Use `--initial-pass-repair-max-chunks-per-burst` to keep first-pass forward data
dominant while still servicing queued repairs in near-real-time.

Periodic transfer metadata is enabled by default every 10 seconds:

```bash
uv run ssync send ./example.bin --dest-port 9000 --feedback \
  --periodic-metadata-interval-s 10.0 --periodic-metadata-every-n-chunks 1024
```

Availability beacons (default every second; `0` disables):

```bash
uv run ssyncd --bind-port 9000 --beacon-interval-s 1.0
uv run ssync send ./example.bin --dest-port 9000 --feedback --beacon-interval-s 1.0
```

Pre-metadata buffering controls on receiver/server:

```bash
uv run ssync receive --bind-port 9000 --feedback \
  --pre-metadata-max-pending-bytes 8388608 \
  --pre-metadata-max-pending-bytes-per-transfer 524288 \
  --pre-metadata-max-pending-transfers 128 \
  --pre-metadata-ttl-s 30
```

Receiver state advertisement:

- On repeated `METADATA`, receiver may advertise current `STATUS(INCOMPLETE)` with
  bounded missing ranges to help sender prioritize immediate repairs.
- Receiver emits `STATUS(INCOMPLETE)` as the repair signal.
- In feedback mode, sender enqueues incoming `STATUS(INCOMPLETE)` requests and
  services repairs concurrently during forward data streaming using bounded
  repair bursts.
- If destination already has a hash-matching completed file, receiver short-circuits
  with `STATUS(COMPLETE)` and sender exits early.
- During feedback wait, sender retries `METADATA` on relevant idle windows to recover
  control-context loss on impaired links.
- Receiver can buffer bounded unknown-transfer data chunks before metadata arrives;
  once metadata appears, buffered chunks are replayed and missing ranges are advertised.

Simulate loss to exercise repair:

```bash
uv run ssync send ./example.bin --dest-port 9000 --feedback --drop-every-nth-data 5
```

Run a full local loopback validation script (open-loop + feedback/repair):

```bash
uv run python scripts/run_loopback_tests.py
```

Run the same loopback validation using only the CLI commands:

```bash
./scripts/run_loopback_cli.sh
```

Probe the fastest stable loopback send rate for the current machine/settings:

```bash
uv run python scripts/benchmark_loopback_rate.py --start-bps 1000000 --max-bps 50000000
```

Or test exact candidate rates:

```bash
uv run python scripts/benchmark_loopback_rate.py --rates-bps 5000000,10000000,20000000
```

Probe the largest stable chunk size at a fixed send cap:

```bash
uv run python scripts/benchmark_loopback_chunk_size.py \
  --max-data-rate-bps 20000000 \
  --chunk-sizes 1200,1400,2048,4096,8192,12000,16384
```

Debug with tcpdump:

```bash
bash ./scripts/run_tcpdump_debug.sh
```

See detailed guidance in `docs/tcpdump-debugging.md`.

Monitor active receiver transfers in a TUI:

```bash
uv run ssync monitor --output-dir ./received --refresh-interval-s 0.5
```

The monitor reads receiver journal state and shows active transfer progress,
range counts, smoothed receive throughput, and a 2D hole map for the selected
transfer (`█` full, `▒` partial, `·` missing). Use up/down (or `j`/`k`) to
change selection and `q` to quit.

Traffic emulation with `tc`/`netem`:

```bash
sudo bash ./scripts/run_emulated_scenario.sh
```

See detailed guidance in `docs/traffic-emulation.md`.

## Project layout

- `src/ssync/space_sync/frames.py`: binary framing encode/decode
- `src/ssync/space_sync/manifest.py`: transfer metadata models
- `src/ssync/space_sync/ranges.py`: missing-chunk range tracking/encoding
- `src/ssync/space_sync/sender.py`: sender behavior and repair loop
- `src/ssync/space_sync/receiver.py`: receiver behavior and reassembly
- `src/ssync/space_sync/cli.py`: command line entry points
- `docs/space-sync-design.md`: design assumptions and roadmap
- `docs/draft-space-sync-transport-00.md`: IETF-style protocol draft
- `docs/tcpdump-debugging.md`: packet capture debugging guide
- `docs/traffic-emulation.md`: tc/netem emulation guide and scenario matrix
- `tests/`: protocol and end-to-end tests

## Developer checks

```bash
uv run ruff check .
uv run mypy
uv run pytest -q
```

## Current limitations

- Prototype quality, not production hardened
- no congestion control, cryptographic authentication, or FEC yet
- receiver-side persistent recovery state is supported via journal replay
- file transfer first; stream transport deferred

