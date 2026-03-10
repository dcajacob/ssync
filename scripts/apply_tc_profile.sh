#!/usr/bin/env bash
set -euo pipefail

NS_TX="${NS_TX:-ns_tx}"
NS_RX="${NS_RX:-ns_rx}"
VETH_TX="${VETH_TX:-veth_tx}"
VETH_RX="${VETH_RX:-veth_rx}"
PROFILE="${PROFILE:-leo_nominal}"

DOWN_RATE=""
DOWN_DELAY=""
DOWN_JITTER=""
DOWN_LOSS=""
DOWN_CORRUPT=""
UP_RATE=""
UP_DELAY=""
UP_JITTER=""
UP_LOSS=""
UP_CORRUPT=""

if ! command -v tc >/dev/null 2>&1; then
  echo "missing required command: tc" >&2
  exit 2
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "apply_tc_profile.sh requires root (run with sudo)." >&2
  exit 2
fi

set_profile_defaults() {
  case "${PROFILE}" in
    leo_manual)
      DOWN_RATE="50mbit"
      DOWN_DELAY="220ms"
      DOWN_JITTER="10ms"
      DOWN_LOSS="2.0%"
      DOWN_CORRUPT="0.03%"
      UP_RATE="256kbit"
      UP_DELAY="220ms"
      UP_JITTER="25ms"
      UP_LOSS="2.0%"
      UP_CORRUPT="0.03%"
      ;;
    leo_nominal)
      DOWN_RATE="50mbit"
      DOWN_DELAY="120ms"
      DOWN_JITTER="10ms"
      DOWN_LOSS="0.2%"
      DOWN_CORRUPT="0.01%"
      UP_RATE="256kbit"
      UP_DELAY="220ms"
      UP_JITTER="25ms"
      UP_LOSS="1.0%"
      UP_CORRUPT="0.03%"
      ;;
    leo_stressed)
      DOWN_RATE="20mbit"
      DOWN_DELAY="180ms"
      DOWN_JITTER="30ms"
      DOWN_LOSS="1.5%"
      DOWN_CORRUPT="0.05%"
      UP_RATE="128kbit"
      UP_DELAY="320ms"
      UP_JITTER="60ms"
      UP_LOSS="4.0%"
      UP_CORRUPT="0.10%"
      ;;
    open_loop_harsh)
      DOWN_RATE="8mbit"
      DOWN_DELAY="220ms"
      DOWN_JITTER="60ms"
      DOWN_LOSS="4.0%"
      DOWN_CORRUPT="0.15%"
      UP_RATE="64kbit"
      UP_DELAY="500ms"
      UP_JITTER="100ms"
      UP_LOSS="20.0%"
      UP_CORRUPT="0.30%"
      ;;
    *)
      echo "unknown profile: ${PROFILE}" >&2
      exit 2
      ;;
  esac
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --profile)
        PROFILE="$2"
        shift 2
        ;;
      --down-rate)
        DOWN_RATE="$2"
        shift 2
        ;;
      --down-delay)
        DOWN_DELAY="$2"
        shift 2
        ;;
      --down-jitter)
        DOWN_JITTER="$2"
        shift 2
        ;;
      --down-loss)
        DOWN_LOSS="$2"
        shift 2
        ;;
      --down-corrupt)
        DOWN_CORRUPT="$2"
        shift 2
        ;;
      --up-rate)
        UP_RATE="$2"
        shift 2
        ;;
      --up-delay)
        UP_DELAY="$2"
        shift 2
        ;;
      --up-jitter)
        UP_JITTER="$2"
        shift 2
        ;;
      --up-loss)
        UP_LOSS="$2"
        shift 2
        ;;
      --up-corrupt)
        UP_CORRUPT="$2"
        shift 2
        ;;
      *)
        echo "unknown argument: $1" >&2
        exit 2
        ;;
    esac
  done
}

apply_direction() {
  local ns="$1"
  local iface="$2"
  local rate="$3"
  local delay="$4"
  local jitter="$5"
  local loss="$6"
  local corrupt="$7"

  tc -n "${ns}" qdisc del dev "${iface}" root 2>/dev/null || true
  tc -n "${ns}" qdisc add dev "${iface}" root handle 1: htb default 10
  tc -n "${ns}" class add dev "${iface}" parent 1: classid 1:10 htb rate "${rate}" ceil "${rate}"
  tc -n "${ns}" qdisc add dev "${iface}" parent 1:10 handle 10: netem \
    delay "${delay}" "${jitter}" distribution normal \
    loss "${loss}" \
    corrupt "${corrupt}"
}

set_profile_defaults
parse_args "$@"

apply_direction "${NS_TX}" "${VETH_TX}" "${DOWN_RATE}" "${DOWN_DELAY}" "${DOWN_JITTER}" "${DOWN_LOSS}" "${DOWN_CORRUPT}"
apply_direction "${NS_RX}" "${VETH_RX}" "${UP_RATE}" "${UP_DELAY}" "${UP_JITTER}" "${UP_LOSS}" "${UP_CORRUPT}"

echo "Applied tc profile '${PROFILE}':"
echo "  downlink (${NS_TX}/${VETH_TX}): rate=${DOWN_RATE} delay=${DOWN_DELAY} jitter=${DOWN_JITTER} loss=${DOWN_LOSS} corrupt=${DOWN_CORRUPT}"
echo "  uplink   (${NS_RX}/${VETH_RX}): rate=${UP_RATE} delay=${UP_DELAY} jitter=${UP_JITTER} loss=${UP_LOSS} corrupt=${UP_CORRUPT}"
