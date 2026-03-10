# Traffic Emulation for Space Sync

This guide provides a repeatable Linux lab for emulating satellite-like links with:

- asymmetric downlink/uplink rates
- delay and jitter
- random and bursty loss
- packet corruption approximation
- intermittent contact windows

The workflow is built around `tc`/`netem`, network namespaces, and `ssync` CLI tools.

## Overview

Core components:

- namespace lab setup: `scripts/setup_netns_lab.sh`
- profile application: `scripts/apply_tc_profile.sh`
- profile clearing: `scripts/clear_tc_profile.sh`
- contact scheduling: `scripts/run_contact_schedule.sh`
- scenario orchestration: `scripts/run_emulated_scenario.sh`

Default deterministic topology:

- sender namespace: `ns_tx`
- receiver namespace: `ns_rx`
- sender interface: `veth_tx` (`10.23.0.1/30`)
- receiver interface: `veth_rx` (`10.23.0.2/30`)

## Prerequisites

- Linux with `iproute2` (`ip`, `tc`)
- `tcpdump`
- root privileges (`sudo`) for namespace/qdisc operations
- `uv` + project dependencies (`uv sync --dev`)

## 1) Setup and teardown

Create lab topology:

```bash
sudo bash ./scripts/setup_netns_lab.sh
```

Remove lab topology:

```bash
sudo bash ./scripts/teardown_netns_lab.sh
```

## 2) Apply and clear tc profiles

Apply a built-in profile:

```bash
sudo PROFILE=leo_nominal bash ./scripts/apply_tc_profile.sh
```

Supported profiles:

- `leo_manual`
- `leo_nominal`
- `leo_stressed`
- `open_loop_harsh`

Override directional parameters:

```bash
sudo bash ./scripts/apply_tc_profile.sh \
  --profile leo_nominal \
  --down-rate 80mbit --down-delay 100ms --down-jitter 15ms \
  --up-rate 128kbit --up-delay 350ms --up-jitter 80ms
```

Clear shaping:

```bash
sudo bash ./scripts/clear_tc_profile.sh
```

## 3) Contact window simulation

Intermittent contact cycles:

```bash
sudo PATTERN=intermittent PROFILE=leo_nominal UP_SECONDS=20 DOWN_SECONDS=10 CYCLES=3 \
  bash ./scripts/run_contact_schedule.sh
```

Delayed uplink availability:

```bash
sudo PATTERN=delayed_uplink PROFILE=leo_nominal DELAYED_UPLINK_SECONDS=30 \
  bash ./scripts/run_contact_schedule.sh
```

Escalating degradation across a pass:

```bash
sudo PATTERN=escalating ESCALATE_STAGE_SECONDS=20 bash ./scripts/run_contact_schedule.sh
```

## 4) End-to-end emulated scenario runner

Run one full scenario with captures and `tc -s` stats:

```bash
sudo PROFILE=leo_nominal FEEDBACK=1 bash ./scripts/run_emulated_scenario.sh
```

Open-loop run:

```bash
sudo PROFILE=open_loop_harsh FEEDBACK=0 bash ./scripts/run_emulated_scenario.sh
```

Manual profile run:

```bash
sudo PROFILE=leo_manual FEEDBACK=1 bash ./scripts/run_emulated_scenario.sh
```

Artifacts include:

- sender JSON result
- tx/rx pcap files
- pre/post `tc -s qdisc` outputs

## 5) Scenario matrix (recommended)

Use this as a repeatable validation matrix:

| Scenario | Profile | Feedback | Expected |
|---|---|---|---|
| Nominal asymmetric | `leo_nominal` | on | complete delivery with low repair rounds |
| Constrained uplink | `leo_stressed` | on | completion with higher repair and longer completion time |
| Harsh open-loop | `open_loop_harsh` | off | repeated rounds needed; compare completion over rounds |
| Intermittent contacts | `leo_nominal` + `PATTERN=intermittent` | on/off | deferred repair behavior should improve completion when uplink reappears |
| Delayed return path | `PATTERN=delayed_uplink` | on | initial forward-only phase then repair once uplink appears |

Track per run:

- completion status (`ssync --json`)
- repair rounds/chunks
- packet counts from pcap
- queue drops/requeues from `tc -s`
- sender post-FIN behavior (`post_fin_timeout`, `status=COMPLETE`, `received_transfer_complete`)

## Notes and caveats

- `netem corrupt` approximates packet corruption and does not model true PHY BER.
- This setup shapes egress in each namespace direction (sufficient for directional asymmetry in this veth topology).
- Scripts are idempotent where practical and include cleanup; still run teardown after abnormal exits.
- On highly impaired return links, increase sender tolerance while testing:
  - `--feedback-wait-s 3`
  - `--max-feedback-idle-timeouts 10`
