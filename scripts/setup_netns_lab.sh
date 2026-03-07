#!/usr/bin/env bash
set -euo pipefail

# Deterministic namespace and interface names.
NS_TX="${NS_TX:-ns_tx}"
NS_RX="${NS_RX:-ns_rx}"
VETH_TX="${VETH_TX:-veth_tx}"
VETH_RX="${VETH_RX:-veth_rx}"
TX_ADDR="${TX_ADDR:-10.23.0.1/30}"
RX_ADDR="${RX_ADDR:-10.23.0.2/30}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

cleanup_partial() {
  ip netns del "${NS_TX}" 2>/dev/null || true
  ip netns del "${NS_RX}" 2>/dev/null || true
}

require_cmd ip

if [[ "${EUID}" -ne 0 ]]; then
  echo "setup_netns_lab.sh requires root (run with sudo)." >&2
  exit 2
fi

cleanup_partial

ip netns add "${NS_TX}"
ip netns add "${NS_RX}"

ip link add "${VETH_TX}" type veth peer name "${VETH_RX}"
ip link set "${VETH_TX}" netns "${NS_TX}"
ip link set "${VETH_RX}" netns "${NS_RX}"

ip -n "${NS_TX}" addr add "${TX_ADDR}" dev "${VETH_TX}"
ip -n "${NS_RX}" addr add "${RX_ADDR}" dev "${VETH_RX}"

ip -n "${NS_TX}" link set lo up
ip -n "${NS_RX}" link set lo up
ip -n "${NS_TX}" link set "${VETH_TX}" up
ip -n "${NS_RX}" link set "${VETH_RX}" up

echo "Created namespaces and veth pair:"
echo "  ${NS_TX}:${VETH_TX} ${TX_ADDR}"
echo "  ${NS_RX}:${VETH_RX} ${RX_ADDR}"
echo
echo "Quick validation:"
echo "  ip netns exec ${NS_TX} ping -c 1 ${RX_ADDR%/*}"
echo "  ip netns exec ${NS_RX} ping -c 1 ${TX_ADDR%/*}"
