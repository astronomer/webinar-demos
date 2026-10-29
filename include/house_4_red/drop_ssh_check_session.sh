#!/bin/sh
# Usage: drop_ssh_check_session.sh [container] [seconds_into_check_loop]
# Waits for the first SSH session running the kill -0 check loop on the target,
# then kills that session's sshd process once, so the client sees a dropped connection.
set -eu

CONTAINER="${1:-house_4_red_ssh_target}"
DELAY="${2:-8}"

while :; do
    pid=$(docker exec "$CONTAINER" pgrep -f '[w]hile kill -0' | head -n 1 || true)
    [ -n "$pid" ] && break
    sleep 1
done

sleep "$DELAY"
sshd_pid=$(docker exec "$CONTAINER" awk '{print $4}' "/proc/$pid/stat")
docker exec "$CONTAINER" kill "$sshd_pid"
echo "Dropped SSH session $sshd_pid (check loop pid $pid) on $CONTAINER"
