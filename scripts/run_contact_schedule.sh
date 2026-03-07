#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PATTERN="${PATTERN:-intermittent}"
PROFILE="${PROFILE:-leo_nominal}"
UP_SECONDS="${UP_SECONDS:-20}"
DOWN_SECONDS="${DOWN_SECONDS:-10}"
CYCLES="${CYCLES:-3}"
DELAYED_UPLINK_SECONDS="${DELAYED_UPLINK_SECONDS:-30}"
ESCALATE_STAGE_SECONDS="${ESCALATE_STAGE_SECONDS:-20}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "run_contact_schedule.sh requires root (run with sudo)." >&2
  exit 2
fi

apply_profile() {
  local profile="$1"
  ./scripts/apply_tc_profile.sh --profile "${profile}"
}

apply_blackout() {
  ./scripts/apply_tc_profile.sh \
    --profile "${PROFILE}" \
    --down-loss "100%" \
    --up-loss "100%" \
    --down-corrupt "0%" \
    --up-corrupt "0%" \
    --down-delay "1ms" \
    --up-delay "1ms" \
    --down-jitter "0ms" \
    --up-jitter "0ms"
}

run_intermittent() {
  for _ in $(seq 1 "${CYCLES}"); do
    apply_profile "${PROFILE}"
    sleep "${UP_SECONDS}"
    apply_blackout
    sleep "${DOWN_SECONDS}"
  done
}

run_delayed_uplink() {
  # Start with downlink available but uplink effectively absent.
  ./scripts/apply_tc_profile.sh \
    --profile "${PROFILE}" \
    --up-loss "100%" \
    --up-corrupt "0%"
  sleep "${DELAYED_UPLINK_SECONDS}"
  apply_profile "${PROFILE}"
}

run_escalating() {
  apply_profile "leo_nominal"
  sleep "${ESCALATE_STAGE_SECONDS}"
  apply_profile "leo_stressed"
  sleep "${ESCALATE_STAGE_SECONDS}"
  apply_profile "open_loop_harsh"
  sleep "${ESCALATE_STAGE_SECONDS}"
}

case "${PATTERN}" in
  intermittent)
    run_intermittent
    ;;
  delayed_uplink)
    run_delayed_uplink
    ;;
  escalating)
    run_escalating
    ;;
  *)
    echo "unknown PATTERN: ${PATTERN}" >&2
    echo "supported: intermittent, delayed_uplink, escalating" >&2
    exit 2
    ;;
esac

echo "Completed contact schedule pattern '${PATTERN}'."
