# Deployment Notes

`ssync` is designed for RF and other impaired links where packet loss, high
latency, asymmetric return paths, and intermittent availability are expected.
Protocol authentication and encryption are intentionally outside the transport:
run `ssync` over a trusted path such as SSH tunneling, IPsec, WireGuard, a VPN,
or an already isolated mission network.

## Trust Boundary

- Treat the UDP sender and receiver as trusted peers on a protected network.
- Do not expose a receiver directly to untrusted networks.
- `ssync` validates framing, bounds repair ranges, and protects receiver-local
  filesystem operations, but it does not authenticate peers or encrypt payloads.
- File-info queries are suitable only within the same trusted boundary.

## Representative Setups

SSH local forwarding:

```bash
ssh -L 9000:127.0.0.1:9000 destination-host
uv run ssync server --bind-host 127.0.0.1 --bind-port 9000 --root-dir ./received
uv run ssync ./payload.bin 127.0.0.1:incoming/payload.bin --dest-port 9000
```

Encrypted site-to-site links:

```bash
# Destination on a VPN/IPsec-protected interface.
uv run ssyncd --bind-host 0.0.0.0 --bind-port 9000 --root-dir /srv/ssync/received

# Source inside the same protected network.
uv run ssync -r ./payloads 10.10.0.20:missions/pass-001/ --dest-port 9000
```

## RF Impairment Tuning

- Use `--max-data-rate-bps` to keep the transmit stream below the link budget.
- Add `--inter-packet-delay-s` when burst loss appears at the receiver.
- For one-way or unreliable return paths, use `--no-feedback` and multiple
  open-loop rounds.
- For intermittent return paths, leave auto feedback enabled so the sender can
  promote to repair mode when receiver beacons or status packets are observed.
- Increase `--feedback-wait-s` and idle timeouts for high-latency paths.

## Receiver Resource Limits

Receivers reject manifests before allocating part files when limits would be
exceeded. Defaults are:

- `max_file_size_bytes = 8589934592` (8 GiB per file)
- `max_active_transfers = 64`
- `max_active_allocation_bytes = 34359738368` (32 GiB aggregate active files)

Configure these in `receive` or `server` sections of a config file, or pass the
corresponding hidden CLI flags for controlled deployments.

## Directory Permissions

- Run the receiver as a dedicated user where possible.
- Make the output directory owned by that user and writable only by trusted
  operators.
- Avoid placing the receiver root inside a world-writable directory.
- `ssync` rejects symlink components under the receiver root so an incoming
  manifest cannot redirect writes outside the configured directory.

## Operational Checks

- Run `uv run pytest` before deployment changes.
- Run `uv run python scripts/run_loopback_tests.py` on deployment hosts to
  validate local loopback behavior.
- Keep `ssync.example.toml` as an example only; copy it to `./.ssync.toml`,
  `~/.ssync.toml`, or `~/.config/ssync/config.toml` when local defaults are
  intentional.
