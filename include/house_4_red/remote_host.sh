#!/bin/sh
# Usage: remote_host.sh ps     live view of running send_invoices jobs, Ctrl+C to stop
#        remote_host.sh out    invoices sent by the newest run, sorted by invoice number
set -eu

CONTAINER="${CONTAINER:-house_4_red_ssh_target}"

case "${1:-}" in
    ps)
        while :; do
            clear
            docker exec "$CONTAINER" ps -eo pid,ppid,etime,args | grep -E 'PID|[s]h /opt/jobs/send_invoices' || true
            sleep 1
        done
        ;;
    out)
        newest=$(docker exec "$CONTAINER" sh -c 'ls -t /config/outbox/*.log | head -n 1')
        echo "\$ sort -k2,2 -s $newest"
        docker exec "$CONTAINER" sort -k2,2 -s "$newest"
        ;;
    *)
        echo "Usage: $0 ps|out" >&2
        exit 1
        ;;
esac
