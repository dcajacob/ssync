#!/usr/bin/env bash
set -euo pipefail

NS_TX="${NS_TX:-ns_tx}"
NS_RX="${NS_RX:-ns_rx}"

if ! command -v ip >/dev/null 2>&1; then
  echo "missing required command: ip" >&2
  exit 2
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "teardown_netns_lab.sh requires root (run with sudo)." >&2
  exit 2
fi

ip netns del "${NS_TX}" 2>/dev/null || true
ip netns del "${NS_RX}" 2>/dev/null || true

echo "Removed namespaces: ${NS_TX}, ${NS_RX}"
