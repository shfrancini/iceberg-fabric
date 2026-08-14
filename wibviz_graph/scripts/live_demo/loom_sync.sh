#!/usr/bin/env bash
set -euo pipefail

SLICE_NAME="${SLICE_NAME:-dune-udp-demo-0ba777}"
NODE_NAME="${NODE_NAME:-receiver}"
REMOTE_FILE="${REMOTE_FILE:-/home/ubuntu/wibviz/wibviz_graph/replay-live-current.csv.gz}"
LOCAL_FILE="${LOCAL_FILE:-/tmp/replay-live-current.csv.gz}"
INTERVAL="${INTERVAL:-3}"
PID_FILE="${PID_FILE:-/tmp/wibviz-live-sync.pid}"
LOG_FILE="${LOG_FILE:-/tmp/wibviz-live-sync.log}"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

case "${1:-}" in
    start)
        if is_running; then
            echo "LoomAI sync already running as PID $(cat "$PID_FILE")"
            exit 0
        fi

        nohup env \
            SLICE_NAME="$SLICE_NAME" \
            NODE_NAME="$NODE_NAME" \
            REMOTE_FILE="$REMOTE_FILE" \
            LOCAL_FILE="$LOCAL_FILE" \
            INTERVAL="$INTERVAL" \
            PID_FILE="$PID_FILE" \
            LOG_FILE="$LOG_FILE" \
            "$SCRIPT_PATH" run >"$LOG_FILE" 2>&1 &
        echo "$!" > "$PID_FILE"
        echo "LoomAI sync started as PID $!"
        echo "Local snapshot: $LOCAL_FILE"
        ;;
    run)
        while true; do
            temporary="${LOCAL_FILE}.new.$$"
            rm -f "$temporary"

            if loomai scp "$SLICE_NAME" "$NODE_NAME" \
                --download "$REMOTE_FILE" "$temporary"; then
                if [[ -s "$temporary" ]]; then
                    mv -f "$temporary" "$LOCAL_FILE"
                fi
            fi

            rm -f "$temporary"
            sleep "$INTERVAL"
        done
        ;;
    stop)
        if is_running; then
            kill "$(cat "$PID_FILE")"
            rm -f "$PID_FILE"
            echo "LoomAI sync stopped"
        else
            echo "LoomAI sync is not running"
            rm -f "$PID_FILE"
        fi
        ;;
    status)
        if is_running; then
            echo "LoomAI sync running as PID $(cat "$PID_FILE")"
            ls -lh "$LOCAL_FILE" 2>/dev/null || true
        else
            echo "LoomAI sync is not running"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status}" >&2
        exit 2
        ;;
esac

