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

Run destination daemon (open-loop mode):

```bash
uv run ssyncd --bind-host 127.0.0.1 --bind-port 9000 --root-dir ./received --no-feedback
```

Send a file:

```bash
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000
```

Run feedback/repair mode:

```bash
uv run ssyncd --bind-port 9000 --root-dir ./received --feedback
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000 --feedback
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
- Default auto-feedback demotion window is `60s` of uplink inactivity (built into the
  sender; not a separate CLI flag).

`--help` shows the common workflow and transport options. Many repair/metadata/debug
knobs are still accepted on the command line for compatibility but are hidden from
help; put them in `./.ssync.toml`, `~/.ssync.toml`, or `~/.config/ssync/config.toml`
under the matching command section when you want tuning without long invocations
(see allowed keys in `src/ssync/space_sync/config_file.py`).

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

Open-loop behavior (`--no-feedback`) is round-based. By default `--open-loop-max-rounds`
is `10`: after each pass over the file set, `ssync` starts another round until that
cap is reached. Use `--open-loop-max-rounds 0` to run rounds continuously. It keeps
a persistent send-state file (`.ssync-open-loop-state.json`) with retransmission counts
and orders each round so files with the lowest retransmission count are sent first.
When feedback becomes active, a revisit worker services queued incomplete
transfers on a `50ms` poll interval and prioritizes the current transfer first.

```bash
# run open-loop continuously (unlimited rounds)
uv run ssync -r --no-feedback --open-loop-max-rounds 0 ./payloads 127.0.0.1:missions/pass-001/

# run exactly two open-loop rounds
uv run ssync -r --no-feedback --open-loop-max-rounds 2 ./payloads 127.0.0.1:missions/pass-001/
```

Machine-readable output for automation:

```bash
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000 --json
uv run ssync -r ./payloads 127.0.0.1:missions/pass-001/ --dest-port 9000 --json
```

Feedback mode timing controls:

```bash
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000 --feedback \
  --feedback-wait-s 3.0 --max-feedback-idle-timeouts 10 --max-repair-rounds 32 \
  --repair-worker-max-chunks-per-burst 256 \
  --initial-pass-repair-max-chunks-per-burst 16
```

Use `--initial-pass-repair-max-chunks-per-burst` to keep first-pass forward data
dominant while still servicing queued repairs in near-real-time.

Sync-mode checksum prefetch:

- `ssync` starts one background checksum worker that precomputes SHA-256 for
  upcoming source files in planned send order to reduce inter-file startup gaps.
- Prefetch is intentionally bounded to one hash at a time (no hash fan-out) to
  avoid uncontrolled CPU/IO amplification on large trees.
- Prefetched hashes are used only when `(path, size, mtime_ns)` still match at
  send time; otherwise sender falls back to inline hashing.
- Sender waits at most about `0.2s` for an in-flight prefetch result before
  falling back to inline hashing, so transfer progress is not blocked by prefetch.

Periodic transfer metadata is enabled by default every 10 seconds:

```bash
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000 --feedback \
  --periodic-metadata-interval-s 10.0 --periodic-metadata-every-n-chunks 1024
```

Availability beacons (default every second; `0` disables):

```bash
uv run ssyncd --bind-port 9000 --beacon-interval-s 1.0
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000 --feedback --beacon-interval-s 1.0
```

Pre-metadata buffering controls on receiver/server:

```bash
uv run ssyncd --bind-port 9000 --root-dir ./received --feedback \
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
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000 --feedback --drop-every-nth-data 5
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

Monitor live IPC (default hybrid mode):

- `ssyncd`/receiver publishes live monitor events over a Unix datagram socket.
- Monitor subscribes to this socket for low-latency updates (including beacon
  strobes) and falls back to journal polling if IPC is unavailable.
- Default socket path is `<output-dir>/.ssync-monitor.sock` (or
  `<root-dir>/.ssync-monitor.sock` for `ssyncd`).
- If IPC is missing/unavailable, receiver continues transfer behavior and monitor
  remains functional via journal polling fallback.
- If a stale datagram socket path is detected (for example, after monitor crash),
  receiver best-effort removes the stale socket path so monitor restart can re-bind.
- Recommended operational model on shared hosts: keep receiver output/root
  directories private (for example `0700`) so local IPC visibility stays scoped.

```bash
# destination daemon publishes monitor IPC here by default:
uv run ssyncd --root-dir ./received

# monitor subscribes to the matching default socket path:
uv run ssync monitor --output-dir ./received

# optional explicit socket override:
uv run ssyncd --root-dir ./received --monitor-ipc-socket /tmp/ssync-monitor.sock
uv run ssync monitor --output-dir ./received --monitor-ipc-socket /tmp/ssync-monitor.sock
```

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

