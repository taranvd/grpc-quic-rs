#!/usr/bin/env bash
# ── netem.sh ── Control packet loss on a network interface via tc.
#
# Usage:
#   scripts/netem.sh on    [iface=lo] [loss%]   — apply loss
#   scripts/netem.sh off   [iface=lo]            — remove loss
#   scripts/netem.sh status [iface=lo]           — show current qdisc
#
# Examples:
#   scripts/netem.sh on lo 5        # 5 % loss on loopback
#   scripts/netem.sh on eth0 1      # 1 % loss on eth0
#   scripts/netem.sh off lo         # restore lo
#   scripts/netem.sh status         # show current state

set -euo pipefail

ACTION="${1:-status}"
IFACE="${2:-lo}"
LOSS="${3:-5}"

case "$ACTION" in
    on)
        sudo tc qdisc replace dev "$IFACE" root netem loss "$LOSS"%
        echo "[netem] ${LOSS}% loss on ${IFACE}"
        ;;
    off)
        sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true
        echo "[netem] loss removed from ${IFACE}"
        ;;
    status)
        echo "--- qdisc on ${IFACE} ---"
        tc qdisc show dev "$IFACE" 2>/dev/null || echo "(none)"
        ;;
    *)
        echo "Usage: $0 {on|off|status} [iface=lo] [loss%=5]"
        exit 1
        ;;
esac
