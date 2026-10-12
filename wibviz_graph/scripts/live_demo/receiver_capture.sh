#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${WIBVIZ_REPO:-$HOME/wibviz/wibviz_graph}"
INTERFACE="${INTERFACE:-enp7s0}"
OUTPUT="${OUTPUT:-$REPO_DIR/replay-live-http.csv}"
MAPPING="${WIBVIZ_CHANNEL_MAP:-$REPO_DIR/event-4245-channel-map.csv}"
TIMEOUT="${TIMEOUT:-180}"
LIMIT="${LIMIT:-640}"
BPF_FILTER="${BPF_FILTER:-udp and dst port 2049}"
TPC="${TPC:-0}"
PLANE="${PLANE:-0}"

if [[ ! -d "$REPO_DIR/src/wibviz" ]]; then
    echo "WibViz source not found at $REPO_DIR" >&2
    exit 1
fi

if [[ ! -f "$MAPPING" ]]; then
    echo "Channel map not found at $MAPPING" >&2
    exit 1
fi

args=(
    --interface "$INTERFACE"
    --output "$OUTPUT"
    --mapping "$MAPPING"
    --tpc "$TPC"
    --plane "$PLANE"
    --timeout "$TIMEOUT"
    --bpf-filter "$BPF_FILTER"
    --verbose
)

if [[ -n "$LIMIT" ]]; then
    args+=(--limit "$LIMIT")
fi

echo "Receiver capture"
echo "  interface: $INTERFACE"
echo "  output:    $OUTPUT"
echo "  mapping:   $MAPPING"
echo "  filter:    $BPF_FILTER"

cd "$REPO_DIR"
exec sudo env \
    "PYTHONPATH=$REPO_DIR/src:${PYTHONPATH:-}" \
    "WIBVIZ_CHANNEL_MAP=$MAPPING" \
    /usr/bin/python3 -m wibviz.export "${args[@]}"

