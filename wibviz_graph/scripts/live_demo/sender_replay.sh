#!/usr/bin/env bash
set -euo pipefail

PCAP="${PCAP:-/home/ubuntu/icecosmich_run82134054_sub001_evt004245.pcap}"
INTERFACE="${INTERFACE:-enp7s0}"
PPS="${PPS:-5}"

if [[ ! -f "$PCAP" ]]; then
    echo "PCAP not found at $PCAP" >&2
    exit 1
fi

echo "Sender replay"
echo "  PCAP:      $PCAP"
echo "  interface: $INTERFACE"
echo "  rate:      $PPS packet(s)/second"

exec sudo tcpreplay --pps="$PPS" -i "$INTERFACE" "$PCAP"

