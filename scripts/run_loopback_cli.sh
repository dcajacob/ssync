#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

CHUNK_SIZE="${CHUNK_SIZE:-256}"
DROP_EVERY_NTH_DATA="${DROP_EVERY_NTH_DATA:-5}"
OPEN_LOOP_ATTEMPTS="${OPEN_LOOP_ATTEMPTS:-3}"

TMP_DIR="$(mktemp -d -t ssync-cli-loopback-XXXXXX)"
OPEN_RX_DIR="${TMP_DIR}/rx-open"
REPAIR_RX_DIR="${TMP_DIR}/rx-repair"
SRC_OPEN="${TMP_DIR}/source-open.bin"
SRC_REPAIR="${TMP_DIR}/source-repair.bin"
RECEIVER_PID=""

cleanup() {
  if [[ -n "${RECEIVER_PID}" ]] && kill -0 "${RECEIVER_PID}" 2>/dev/null; then
    kill "${RECEIVER_PID}" 2>/dev/null || true
    wait "${RECEIVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

free_port() {
  python - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

start_receiver() {
  local port="$1"
  local out_dir="$2"
  local feedback_flag="${3:---no-feedback}"
  uv run ssyncd --bind-host 127.0.0.1 --bind-port "${port}" --root-dir "${out_dir}" "${feedback_flag}" >/dev/null 2>&1 &
  RECEIVER_PID="$!"
  # Allow receiver thread/socket to come up.
  sleep 0.20
}

stop_receiver() {
  if [[ -n "${RECEIVER_PID}" ]] && kill -0 "${RECEIVER_PID}" 2>/dev/null; then
    kill "${RECEIVER_PID}" 2>/dev/null || true
    wait "${RECEIVER_PID}" 2>/dev/null || true
  fi
  RECEIVER_PID=""
}

wait_for_file() {
  local path="$1"
  local timeout_s="${2:-8}"
  python - "$path" "$timeout_s" <<'PY'
import sys
import time
from pathlib import Path
path = Path(sys.argv[1])
deadline = time.time() + float(sys.argv[2])
while time.time() < deadline:
    if path.exists():
        print("ready")
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit(1)
PY
}

hash_file() {
  sha256sum "$1" | awk '{print $1}'
}

echo "Preparing demo payloads..."
python - <<PY
from pathlib import Path
Path("${SRC_OPEN}").write_bytes(b"space-sync-open-loop-" * 4096)
Path("${SRC_REPAIR}").write_bytes(b"space-sync-feedback-repair-" * 4096)
PY

echo "Space Sync CLI loopback scenarios"
echo "================================="

# Scenario 1: open-loop
open_port="$(free_port)"
mkdir -p "${OPEN_RX_DIR}"
start_receiver "${open_port}" "${OPEN_RX_DIR}" "--no-feedback"

open_passed=0
open_details="receiver did not produce output file"
for attempt in $(seq 1 "${OPEN_LOOP_ATTEMPTS}"); do
  rm -f "${OPEN_RX_DIR}/$(basename "${SRC_OPEN}")"
  uv run ssync "${SRC_OPEN}" "127.0.0.1:$(basename "${SRC_OPEN}")" \
    --dest-port "${open_port}" \
    --chunk-size "${CHUNK_SIZE}" \
    --manifest-repeats 5 \
    --inter-packet-delay-s 0.0005 >/dev/null

  if ! wait_for_file "${OPEN_RX_DIR}/$(basename "${SRC_OPEN}")" 8 >/dev/null 2>&1; then
    open_details="attempt ${attempt}: receiver did not produce output file"
    continue
  fi

  src_hash="$(hash_file "${SRC_OPEN}")"
  rx_hash="$(hash_file "${OPEN_RX_DIR}/$(basename "${SRC_OPEN}")")"
  if [[ "${src_hash}" != "${rx_hash}" ]]; then
    open_details="attempt ${attempt}: hash mismatch"
    continue
  fi

  open_passed=1
  open_details="attempt=${attempt} hash=${src_hash}"
  break
done
stop_receiver

# Scenario 2: feedback + repair
repair_port="$(free_port)"
mkdir -p "${REPAIR_RX_DIR}"
start_receiver "${repair_port}" "${REPAIR_RX_DIR}" "--feedback"

feedback_passed=0
feedback_details="receiver did not produce output file"
send_output="$(
  uv run ssync "${SRC_REPAIR}" "127.0.0.1:$(basename "${SRC_REPAIR}")" \
    --dest-port "${repair_port}" \
    --chunk-size "${CHUNK_SIZE}" \
    --manifest-repeats 5 \
    --inter-packet-delay-s 0.0005 \
    --feedback \
    --drop-every-nth-data "${DROP_EVERY_NTH_DATA}" \
    --max-repair-rounds 3 \
    --feedback-wait-s 4.0 \
    --max-feedback-idle-timeouts 4 || true
)"

if wait_for_file "${REPAIR_RX_DIR}/$(basename "${SRC_REPAIR}")" 8 >/dev/null 2>&1; then
  src_hash="$(hash_file "${SRC_REPAIR}")"
  rx_hash="$(hash_file "${REPAIR_RX_DIR}/$(basename "${SRC_REPAIR}")")"
  if [[ "${src_hash}" == "${rx_hash}" ]]; then
    feedback_passed=1
    feedback_details="${send_output}"
  else
    feedback_details="hash mismatch"
  fi
fi
stop_receiver

if [[ "${open_passed}" -eq 1 ]]; then
  echo "[PASS] open-loop: ${open_details}"
else
  echo "[FAIL] open-loop: ${open_details}"
fi

if [[ "${feedback_passed}" -eq 1 ]]; then
  echo "[PASS] feedback-repair: ${feedback_details}"
else
  echo "[FAIL] feedback-repair: ${feedback_details}"
fi

if [[ "${open_passed}" -eq 1 && "${feedback_passed}" -eq 1 ]]; then
  exit 0
fi

exit 1

