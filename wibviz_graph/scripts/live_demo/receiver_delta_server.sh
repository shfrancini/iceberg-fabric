#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${WIBVIZ_REPO:-$HOME/wibviz/wibviz_graph}"
CSV_FILE="${CSV_FILE:-$REPO_DIR/replay-live-http.csv}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8063}"
MAX_BYTES="${MAX_BYTES:-262144}"
PID_FILE="${PID_FILE:-$REPO_DIR/live-delta-server.pid}"
LOG_FILE="${LOG_FILE:-$REPO_DIR/live-delta-server.log}"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

case "${1:-}" in
    start)
        if is_running; then
            echo "Delta server already running as PID $(cat "$PID_FILE")"
            exit 0
        fi

        nohup env \
            WIBVIZ_REPO="$REPO_DIR" \
            CSV_FILE="$CSV_FILE" \
            HOST="$HOST" \
            PORT="$PORT" \
            MAX_BYTES="$MAX_BYTES" \
            PID_FILE="$PID_FILE" \
            LOG_FILE="$LOG_FILE" \
            "$SCRIPT_PATH" run >"$LOG_FILE" 2>&1 &
        echo "$!" > "$PID_FILE"
        echo "Delta server started as PID $!"
        echo "Receiver endpoint: http://127.0.0.1:$PORT/"
        ;;
    run)
        cd "$REPO_DIR"
        exec env \
            "PYTHONPATH=$REPO_DIR/src:${PYTHONPATH:-}" \
            /usr/bin/python3 -m wibviz.delta_server \
            --csv "$CSV_FILE" \
            --host "$HOST" \
            --port "$PORT" \
            --max-bytes "$MAX_BYTES"
        ;;
    stop)
        if is_running; then
            kill "$(cat "$PID_FILE")"
            rm -f "$PID_FILE"
            echo "Delta server stopped"
        else
            echo "Delta server is not running"
            rm -f "$PID_FILE"
        fi
        ;;
    status)
        if is_running; then
            echo "Delta server running as PID $(cat "$PID_FILE")"
            curl -sS -I "http://127.0.0.1:$PORT/" | sed -n '1,12p'
        else
            echo "Delta server is not running"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status}" >&2
        exit 2
        ;;
esac
