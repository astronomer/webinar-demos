#!/bin/sh
# Usage: send_invoices.sh <run_id>
# Stand-in for a billing job on the remote host: waits, then "sends" one invoice per customer
# by appending it to /config/outbox/<run_id>.log.
set -eu

run_id="$1"
outbox=/config/outbox
mkdir -p "$outbox"

sleep 45

sent_at=$(date +%H:%M:%S)
while IFS='|' read -r invoice customer amount; do
    printf '%s  %s  %-24s  EUR %8s  pid=%s\n' "$sent_at" "$invoice" "$customer" "$amount" "$PPID" >> "$outbox/$run_id.log"
done <<'EOF'
INV-2026-10-001|Pumpkin Patch Co|1,290.00
INV-2026-10-002|Haunted Hollow Hotels|4,750.00
INV-2026-10-003|Cobweb Cleaning Ltd|312.50
INV-2026-10-004|Witching Hour Watches|2,080.00
INV-2026-10-005|Crypt & Co Storage|960.00
EOF
