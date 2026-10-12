#!/usr/bin/env bash
set -euo pipefail

FILE="${1:-replay-live-http.csv}"
EXPECTED_LINES="${EXPECTED_LINES:-409601}"
EXPECTED_HEADER="seq_id,timestamp,tick,adc_value,wire_id,tpc,slot,plane,packet_file"

if [[ ! -f "$FILE" ]]; then
    echo "Output not found: $FILE" >&2
    exit 1
fi

if [[ "$FILE" == *.gz ]]; then
    gzip -t "$FILE"
    line_count="$(gzip -dc "$FILE" | wc -l | tr -d ' ')"
    header="$(gzip -dc "$FILE" | sed -n '1p' | tr -d '\r')"
else
    line_count="$(wc -l < "$FILE" | tr -d ' ')"
    header="$(sed -n '1p' "$FILE" | tr -d '\r')"
fi

echo "File:   $FILE"
echo "Lines:  $line_count"
echo "Rows:   $((line_count - 1))"

if [[ "$header" != "$EXPECTED_HEADER" ]]; then
    echo "Unexpected CSV header: $header" >&2
    exit 1
fi

if [[ "$line_count" -ne "$EXPECTED_LINES" ]]; then
    echo "Expected $EXPECTED_LINES lines" >&2
    exit 1
fi

echo "Validation passed"

