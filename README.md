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
- dependency-light, standard library implementation
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

# source host
uv run ssync sync ./example.bin 127.0.0.1:incoming/example.bin --dest-port 9000
```

Sync a directory tree to a destination root path:

```bash
uv run ssync sync ./payloads 127.0.0.1:missions/pass-001/ --dest-port 9000
```

The `sync` command enables repair feedback by default. Use `--no-feedback` for strict
open-loop behavior.

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

## Project layout

- `src/ssync/space_sync/frames.py`: binary framing encode/decode
- `src/ssync/space_sync/manifest.py`: transfer metadata models
- `src/ssync/space_sync/ranges.py`: missing-chunk range tracking/encoding
- `src/ssync/space_sync/sender.py`: sender behavior and repair loop
- `src/ssync/space_sync/receiver.py`: receiver behavior and reassembly
- `src/ssync/space_sync/cli.py`: command line entry points
- `docs/space-sync-design.md`: design assumptions and roadmap
- `docs/draft-space-sync-transport-00.md`: IETF-style protocol draft
- `tests/`: protocol and end-to-end tests

## Current limitations

- Prototype quality, not production hardened
- no congestion control, cryptographic authentication, or FEC yet
- no persistent recovery state across process restarts
- file transfer first; stream transport deferred

