#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PORT="${PORT:-9000}"
INTERFACE="${INTERFACE:-lo}"
CAPTURE_PREFIX="${CAPTURE_PREFIX:-}"
TMP_DIR="$(mktemp -d -t ssync-tcpdump-XXXXXX)"
RECEIVER_PID=""
TCPDUMP_PID=""

PCAP_OUT="${PCAP_OUT:-${ROOT_DIR}/ssync-loopback-${PORT}.pcap}"
SOURCE_FILE="${TMP_DIR}/source.bin"
RECEIVED_DIR="${TMP_DIR}/received"

cleanup() {
  if [[ -n "${TCPDUMP_PID}" ]] && kill -0 "${TCPDUMP_PID}" 2>/dev/null; then
    kill "${TCPDUMP_PID}" 2>/dev/null || true
    wait "${TCPDUMP_PID}" 2>/dev/null || true
  fi
  if [[ -n "${RECEIVER_PID}" ]] && kill -0 "${RECEIVER_PID}" 2>/dev/null; then
    kill "${RECEIVER_PID}" 2>/dev/null || true
    wait "${RECEIVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

if ! command -v tcpdump >/dev/null 2>&1; then
  echo "tcpdump is not installed."
  exit 2
fi

mkdir -p "$(dirname "${PCAP_OUT}")"
mkdir -p "${RECEIVED_DIR}"

python - <<PY
from pathlib import Path
Path("${SOURCE_FILE}").write_bytes(b"ssync-tcpdump-debug-" * 4096)
PY

echo "Starting ssync receiver on 127.0.0.1:${PORT}"
uv run ssyncd --bind-host 127.0.0.1 --bind-port "${PORT}" --root-dir "${RECEIVED_DIR}" --feedback >/dev/null 2>&1 &
RECEIVER_PID="$!"
sleep 0.30

echo "Starting tcpdump capture on interface ${INTERFACE}"
if [[ -n "${CAPTURE_PREFIX}" ]]; then
  # shellcheck disable=SC2086
  ${CAPTURE_PREFIX} tcpdump -i "${INTERFACE}" -nn -s 0 -U -w "${PCAP_OUT}" "udp port ${PORT} and host 127.0.0.1" >/dev/null 2>&1 &
else
  tcpdump -i "${INTERFACE}" -nn -s 0 -U -w "${PCAP_OUT}" "udp port ${PORT} and host 127.0.0.1" >/dev/null 2>&1 &
fi
TCPDUMP_PID="$!"
sleep 0.25

echo "Running sender with induced loss to trigger feedback/repair"
uv run ssync "${SOURCE_FILE}" "127.0.0.1:source.bin" \
  --dest-port "${PORT}" \
  --feedback \
  --drop-every-nth-data 5 \
  --max-repair-rounds 3 \
  --feedback-wait-s 4.0 >/dev/null

sleep 0.5
kill "${TCPDUMP_PID}" 2>/dev/null || true
wait "${TCPDUMP_PID}" 2>/dev/null || true
TCPDUMP_PID=""

kill "${RECEIVER_PID}" 2>/dev/null || true
wait "${RECEIVER_PID}" 2>/dev/null || true
RECEIVER_PID=""

total_packets="$(tcpdump -nn -r "${PCAP_OUT}" 2>/dev/null | wc -l | tr -d ' ')"
to_receiver_packets="$(tcpdump -nn -r "${PCAP_OUT}" "dst port ${PORT}" 2>/dev/null | wc -l | tr -d ' ')"
from_receiver_packets="$(tcpdump -nn -r "${PCAP_OUT}" "src port ${PORT}" 2>/dev/null | wc -l | tr -d ' ')"

echo
echo "Capture complete."
echo "pcap: ${PCAP_OUT}"
echo "packets(total): ${total_packets}"
echo "packets(sender->receiver): ${to_receiver_packets}"
echo "packets(receiver->sender): ${from_receiver_packets}"
echo
echo "Inspect with:"
echo "  tcpdump -nn -r \"${PCAP_OUT}\""
echo "  tcpdump -nn -X -r \"${PCAP_OUT}\""
