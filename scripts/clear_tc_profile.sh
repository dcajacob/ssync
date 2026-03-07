#!/usr/bin/env bash
set -euo pipefail

NS_TX="${NS_TX:-ns_tx}"
NS_RX="${NS_RX:-ns_rx}"
VETH_TX="${VETH_TX:-veth_tx}"
VETH_RX="${VETH_RX:-veth_rx}"

if ! command -v tc >/dev/null 2>&1; then
  echo "missing required command: tc" >&2
  exit 2
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "clear_tc_profile.sh requires root (run with sudo)." >&2
  exit 2
fi

tc -n "${NS_TX}" qdisc del dev "${VETH_TX}" root 2>/dev/null || true
tc -n "${NS_RX}" qdisc del dev "${VETH_RX}" root 2>/dev/null || true

echo "Cleared tc qdisc on ${NS_TX}/${VETH_TX} and ${NS_RX}/${VETH_RX}"
