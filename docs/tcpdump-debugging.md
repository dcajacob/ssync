# Debugging Space Sync with tcpdump

This guide shows how to capture and inspect Space Sync UDP traffic with `tcpdump`.

## Why use tcpdump

Use packet captures when you need to verify:

- sender packets are actually leaving the host
- receiver replies (feedback/repair) are present or missing
- packet rate/order during open-loop or feedback transfers
- whether problems are application-level or network-level

## Prerequisites

- `tcpdump` installed
- permission to capture packets (root, `sudo`, or Linux capabilities)
- Space Sync environment available (`uv sync --dev`)

## Manual capture workflow

Terminal A: start receiver/server:

```bash
uv run ssyncd --bind-host 127.0.0.1 --bind-port 9000 --root-dir ./received
```

Terminal B: start capture:

```bash
sudo tcpdump -i lo -nn -s 0 -U -w ./ssync-loopback.pcap "udp port 9000 and host 127.0.0.1"
```

Terminal C: run sender:

```bash
uv run ssync ./example.bin 127.0.0.1:example.bin --dest-port 9000 --feedback --drop-every-nth-data 5
```

Stop capture with `Ctrl-C` in Terminal B.

Namespace lab capture (when using `ns_tx`/`ns_rx`):

```bash
sudo ip netns exec ns_tx tcpdump -i veth_tx -nn -s 0 -U -w ./ssync-ns-tx.pcap "udp port 9000"
sudo ip netns exec ns_rx tcpdump -i veth_rx -nn -s 0 -U -w ./ssync-ns-rx.pcap "udp port 9000"
```

## Inspecting the capture

Basic packet list:

```bash
tcpdump -nn -r ./ssync-loopback.pcap
```

Show payload bytes:

```bash
tcpdump -nn -X -r ./ssync-loopback.pcap
```

Count packets:

```bash
tcpdump -nn -r ./ssync-loopback.pcap 2>/dev/null | wc -l
```

Direction hints:

```bash
tcpdump -nn -r ./ssync-loopback.pcap "dst port 9000"   # sender -> receiver
tcpdump -nn -r ./ssync-loopback.pcap "src port 9000"   # receiver -> sender
```

## Scripted capture

Use the helper script for repeatable captures:

```bash
bash ./scripts/run_tcpdump_debug.sh
```

Useful overrides:

```bash
PORT=9010 INTERFACE=lo CAPTURE_PREFIX="sudo" bash ./scripts/run_tcpdump_debug.sh
```

Output:

- `.pcap` file path
- packet counts (total, to receiver port, from receiver port)
- suggested decode commands

For namespace-based asymmetric link emulation and `tc -s` stats capture, see
`docs/traffic-emulation.md` and `scripts/run_emulated_scenario.sh`.

## Common issues

- `tcpdump: permission denied`
  - Run with `sudo`, or grant capture capabilities to `tcpdump`.
- No packets captured
  - Wrong interface (for loopback use `lo` on Linux).
  - Wrong port filter.
  - Sender/receiver not actually running.
- Only sender packets, no receiver packets
  - Receiver may not be in feedback mode or not receiving frames.
  - Check server bind host/port and local firewall rules.
- Sender appears stuck after `sent_fin`
  - Capture both directions and verify whether receiver emits
    `STATUS(INCOMPLETE)` with missing ranges, then terminal `STATUS(COMPLETE)`.
  - If return-link impairment is high, increase sender tolerance:
    `--feedback-wait-s 3 --max-feedback-idle-timeouts 10`.
