#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

# sudo resets PATH; uv is often installed under the invoking user's home.
if [[ -n "${SUDO_USER:-}" ]]; then
  _invoker_home="$(getent passwd "${SUDO_USER}" | cut -d: -f6)"
  PATH="${_invoker_home}/.local/bin:${_invoker_home}/.cargo/bin:${PATH}"
  unset _invoker_home
fi
PATH="/usr/local/bin:${PATH}"
export PATH

PROFILE="${PROFILE:-leo_nominal}"
FEEDBACK="${FEEDBACK:-1}"
PORT="${PORT:-9000}"
SCENARIO_NAME="${SCENARIO_NAME:-${PROFILE}}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/artifacts/emulation-${SCENARIO_NAME}-$(date +%Y%m%d-%H%M%S)}"
SOURCE_FILE="${OUTPUT_DIR}/source.bin"
RECEIVED_DIR="${OUTPUT_DIR}/received"
SENDER_JSON="${OUTPUT_DIR}/sender.json"
RECEIVER_LOG="${OUTPUT_DIR}/receiver.log"
TC_BEFORE_TX="${OUTPUT_DIR}/tc-before-tx.txt"
TC_BEFORE_RX="${OUTPUT_DIR}/tc-before-rx.txt"
TC_AFTER_TX="${OUTPUT_DIR}/tc-after-tx.txt"
TC_AFTER_RX="${OUTPUT_DIR}/tc-after-rx.txt"
PCAP_TX="${OUTPUT_DIR}/tx.pcap"
PCAP_RX="${OUTPUT_DIR}/rx.pcap"
KEEP_NETNS="${KEEP_NETNS:-0}"

NS_TX="${NS_TX:-ns_tx}"
NS_RX="${NS_RX:-ns_rx}"
VETH_TX="${VETH_TX:-veth_tx}"
VETH_RX="${VETH_RX:-veth_rx}"
TX_HOST="${TX_HOST:-10.23.0.1}"
RX_HOST="${RX_HOST:-10.23.0.2}"

RECEIVER_PID=""
TCPDUMP_TX_PID=""
TCPDUMP_RX_PID=""

cleanup() {
  if [[ -n "${TCPDUMP_TX_PID}" ]] && kill -0 "${TCPDUMP_TX_PID}" 2>/dev/null; then
    kill "${TCPDUMP_TX_PID}" 2>/dev/null || true
    wait "${TCPDUMP_TX_PID}" 2>/dev/null || true
  fi
  if [[ -n "${TCPDUMP_RX_PID}" ]] && kill -0 "${TCPDUMP_RX_PID}" 2>/dev/null; then
    kill "${TCPDUMP_RX_PID}" 2>/dev/null || true
    wait "${TCPDUMP_RX_PID}" 2>/dev/null || true
  fi
  if [[ -n "${RECEIVER_PID}" ]] && kill -0 "${RECEIVER_PID}" 2>/dev/null; then
    kill "${RECEIVER_PID}" 2>/dev/null || true
    wait "${RECEIVER_PID}" 2>/dev/null || true
  fi
  if [[ "${KEEP_NETNS}" != "1" ]]; then
    ./scripts/teardown_netns_lab.sh >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

if [[ "${EUID}" -ne 0 ]]; then
  echo "run_emulated_scenario.sh requires root (run with sudo)." >&2
  exit 2
fi

require_cmd ip
require_cmd tc
require_cmd tcpdump
if ! command -v uv >/dev/null 2>&1; then
  echo "missing required command: uv; install uv or from your normal shell run: sudo env \"PATH=\$PATH\" bash ./scripts/run_emulated_scenario.sh ..." >&2
  exit 2
fi

mkdir -p "${OUTPUT_DIR}" "${RECEIVED_DIR}"

./scripts/setup_netns_lab.sh
./scripts/apply_tc_profile.sh --profile "${PROFILE}"

python - <<PY
from pathlib import Path
Path("${SOURCE_FILE}").write_bytes(b"ssync-emulation-" * 8192)
PY

ip netns exec "${NS_TX}" tc -s qdisc show dev "${VETH_TX}" >"${TC_BEFORE_TX}"
ip netns exec "${NS_RX}" tc -s qdisc show dev "${VETH_RX}" >"${TC_BEFORE_RX}"

ip netns exec "${NS_TX}" tcpdump -i "${VETH_TX}" -nn -s 0 -U -w "${PCAP_TX}" "udp port ${PORT}" >/dev/null 2>&1 &
TCPDUMP_TX_PID="$!"
ip netns exec "${NS_RX}" tcpdump -i "${VETH_RX}" -nn -s 0 -U -w "${PCAP_RX}" "udp port ${PORT}" >/dev/null 2>&1 &
TCPDUMP_RX_PID="$!"

if [[ "${FEEDBACK}" == "1" ]]; then
  ip netns exec "${NS_RX}" uv run ssyncd \
    --bind-host "${RX_HOST}" \
    --bind-port "${PORT}" \
    --root-dir "${RECEIVED_DIR}" \
    --feedback >"${RECEIVER_LOG}" 2>&1 &
else
  ip netns exec "${NS_RX}" uv run ssyncd \
    --bind-host "${RX_HOST}" \
    --bind-port "${PORT}" \
    --root-dir "${RECEIVED_DIR}" \
    --no-feedback >"${RECEIVER_LOG}" 2>&1 &
fi
RECEIVER_PID="$!"
sleep 0.5
if ! kill -0 "${RECEIVER_PID}" 2>/dev/null; then
  echo "receiver failed to start; see ${RECEIVER_LOG}" >&2
  exit 1
fi

if [[ "${FEEDBACK}" == "1" ]]; then
  ip netns exec "${NS_TX}" uv run ssync "${SOURCE_FILE}" "${RX_HOST}:scenario/input.bin" \
    --dest-port "${PORT}" \
    --feedback \
    --json >"${SENDER_JSON}"
else
  ip netns exec "${NS_TX}" uv run ssync "${SOURCE_FILE}" "${RX_HOST}:scenario/input.bin" \
    --dest-port "${PORT}" \
    --no-feedback \
    --open-loop-max-rounds 1 \
    --json >"${SENDER_JSON}"
fi

sleep 0.5

ip netns exec "${NS_TX}" tc -s qdisc show dev "${VETH_TX}" >"${TC_AFTER_TX}"
ip netns exec "${NS_RX}" tc -s qdisc show dev "${VETH_RX}" >"${TC_AFTER_RX}"

kill "${TCPDUMP_TX_PID}" 2>/dev/null || true
wait "${TCPDUMP_TX_PID}" 2>/dev/null || true
TCPDUMP_TX_PID=""
kill "${TCPDUMP_RX_PID}" 2>/dev/null || true
wait "${TCPDUMP_RX_PID}" 2>/dev/null || true
TCPDUMP_RX_PID=""

kill "${RECEIVER_PID}" 2>/dev/null || true
wait "${RECEIVER_PID}" 2>/dev/null || true
RECEIVER_PID=""

echo "Scenario complete."
echo "  output_dir: ${OUTPUT_DIR}"
echo "  sender_json: ${SENDER_JSON}"
echo "  receiver_log: ${RECEIVER_LOG}"
echo "  pcap_tx: ${PCAP_TX}"
echo "  pcap_rx: ${PCAP_RX}"
echo "  tc_before_tx: ${TC_BEFORE_TX}"
echo "  tc_before_rx: ${TC_BEFORE_RX}"
echo "  tc_after_tx: ${TC_AFTER_TX}"
echo "  tc_after_rx: ${TC_AFTER_RX}"
