#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${WIBVIZ_REPO:-$HOME/wibviz/wibviz_graph}"
SOURCE="${SOURCE:-$REPO_DIR/replay-live-http.csv}"
SNAPSHOT="${SNAPSHOT:-$REPO_DIR/replay-live-current.csv.gz}"
INTERVAL="${INTERVAL:-3}"
PID_FILE="${PID_FILE:-$REPO_DIR/live-snapshot.pid}"
LOG_FILE="${LOG_FILE:-$REPO_DIR/live-snapshot.log}"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

case "${1:-}" in
    start)
        if is_running; then
            echo "Snapshotter already running as PID $(cat "$PID_FILE")"
            exit 0
        fi

        nohup env \
            WIBVIZ_REPO="$REPO_DIR" \
            SOURCE="$SOURCE" \
            SNAPSHOT="$SNAPSHOT" \
            INTERVAL="$INTERVAL" \
            PID_FILE="$PID_FILE" \
            LOG_FILE="$LOG_FILE" \
            "$SCRIPT_PATH" run >"$LOG_FILE" 2>&1 &
        echo "$!" > "$PID_FILE"
        echo "Snapshotter started as PID $!"
        echo "Snapshot: $SNAPSHOT"
        ;;
    run)
        trap 'rm -f "${SNAPSHOT}.tmp.$$"' EXIT
        while true; do
            if [[ -s "$SOURCE" ]]; then
                gzip -1 -c "$SOURCE" > "${SNAPSHOT}.tmp.$$"
                mv -f "${SNAPSHOT}.tmp.$$" "$SNAPSHOT"
            fi
            sleep "$INTERVAL"
        done
        ;;
    stop)
        if is_running; then
            kill "$(cat "$PID_FILE")"
            rm -f "$PID_FILE"
            echo "Snapshotter stopped"
        else
            echo "Snapshotter is not running"
            rm -f "$PID_FILE"
        fi
        ;;
    status)
        if is_running; then
            echo "Snapshotter running as PID $(cat "$PID_FILE")"
            ls -lh "$SNAPSHOT" 2>/dev/null || true
        else
            echo "Snapshotter is not running"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|status}" >&2
        exit 2
        ;;
esac

