#!/usr/bin/env bash
# Helper commands for the event-driven scheduling webinar demo. Run from anywhere; it works in this
# folder. Works with the bash 3.2 that ships with macOS.
set -euo pipefail

cd "$(dirname "$0")"

DEMO_DAG_FILES="orders_kafka.py returns_table.py"
DEMO_DAG_IDS="ship_orders refund_orders restock_returns"
GROUP=airflow-watchers
API=http://localhost:8082/api/v2
LAST_SENT=.last-sent

usage() {
  cat <<EOF
usage: ./demo.sh <command>

  rehearse                   run the whole demo once, unattended (about 5 minutes), so every
                             step has finished runs to show; ends with a reset and a check for
                             failed task attempts
  reset                      back to the starting point: demo1 active, two rows in the returns
                             table, empty asset state store and dead-letter topic, no asset
                             event left waiting for a run (keeps Dag run history)
  use <step>                 activate one step: demo1, demo2-before, demo2, demo3-before, demo3
                             (the second and third demos have a "before the fix" step)

  order <paid|refunded> [id] send one order event to the Kafka "orders" topic
  burst                      send six events at once: three paid orders, then three refunds
  bad-message                send a message that isn't JSON
  received                   the asset events Airflow stored since the last order, burst or
                             return; says so when Kafka still has messages to read
  consumers                  the Kafka consumer group: who reads the topic, the committed offset
  dlq                        the messages in the dead-letter topic (demo2)

  return [order_id] [sku]    a return lands in the returns table (third demo)
  returns                    show the returns table
  cursor                     what the returns watcher keeps in the asset state store (demo3)
  restart-triggerer          restart the triggerer, like a deploy does, and print the cursor the
                             returns watcher starts from (third demo)

  trigger-logs               follow the triggerer logs (where the watchers run)
  wipe-runs                  switch to demo1, then delete the run history of the demo Dags
EOF
}

container() {
  local name
  name=$(docker ps --format '{{.Names}}' | grep -E "^event-driven-demo_.*-$1-1$" | head -1 || true)
  if [[ -z "$name" ]]; then
    echo "no running '$1' container (is 'astro dev start' running?)" >&2
    exit 1
  fi
  echo "$name"
}

kafka() {
  docker exec -i "$(container kafka)" "/opt/kafka/bin/$1" --bootstrap-server localhost:9092 "${@:2}"
}

metadata_psql() {
  docker exec -i "$(container postgres)" psql -U postgres -d postgres "$@"
}

shop_psql() {
  docker exec -i -e PGOPTIONS=--client-min-messages=warning "$(container shop-db)" psql -U shop -d shop "$@"
}

airflow_cli() {
  docker exec -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR "$(container scheduler)" airflow "$@" 2>/dev/null
}

# Runs a short Python snippet on stdin data, inside the scheduler container (no Python needed on
# the laptop).
py() {
  docker exec -i "$(container scheduler)" python -c "$1" "${@:2}"
}

# The local Airflow API needs no login: Astro's dev setup makes everyone an admin.
api() {
  curl -fsS -X "${2:-GET}" "$API$1"
}

asset_id() {
  api "/assets?name_pattern=$1" | py '
import json, sys
print(next(a["id"] for a in json.load(sys.stdin)["assets"] if a["name"] == sys.argv[1]))' "$1"
}

# ---------------------------------------------------------------------------------------------
# Events

CUSTOMERS="ana@example.com ben@example.com chloe@example.com dev@example.com emma@example.com"

# Prints one order event as JSON. The amount and customer come from the id, so a refund matches
# the order it refunds.
order_json() {
  local type=$1 id=$2 customer amount
  customer=$(echo $CUSTOMERS | cut -d' ' -f$((id % 5 + 1)))
  amount=$((id % 7 * 10 + 19)).90
  printf '{"order_id": %s, "type": "%s", "amount": %s, "customer": "%s"}' "$id" "$type" "$amount" "$customer"
}

# Remembers when the last batch went out, how many and where to ("kafka" or "table"), so
# "received" shows what came of it.
mark_sent() {
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $1 $2" > "$LAST_SENT"
}

send() {
  mark_sent "$#" kafka
  printf '%s\n' "$@" | kafka kafka-console-producer.sh --topic orders >/dev/null
  printf 'sent: %s\n' "$@"
}

# A new order id per second, between 2000 and 8999.
new_order_id() {
  echo $(($(date +%s) % 7000 + 2000))
}

burst() {
  local base
  base=$(new_order_id)
  # Three new orders get paid, then three older orders get refunded.
  send "$(order_json paid "$base")" "$(order_json paid $((base + 1)))" "$(order_json paid $((base + 2)))" \
    "$(order_json refunded $((base - 1000)))" "$(order_json refunded $((base - 999)))" \
    "$(order_json refunded $((base - 998)))"
}

add_return() {
  mark_sent 1 table
  shop_psql -c "INSERT INTO returns (order_id, sku, reason)
                VALUES (${1:-$(new_order_id)}, '${2:-SNEAKERS-WHITE-42}', 'changed mind')
                RETURNING id, order_id, sku, reason"
}

# Number of asset events stored since the last send. With an asset name, only that asset's.
stored_count() {
  local since
  since=$(cut -d' ' -f1 "$LAST_SENT")
  metadata_psql -At -c "
    SELECT count(*) FROM asset_event e JOIN asset a ON a.id = e.asset_id
    WHERE e.timestamp >= '$since' AND ('${1:-}' = '' OR a.name = '${1:-}')"
}

received() {
  local since sent kind lag
  if [[ ! -f $LAST_SENT ]]; then
    echo "nothing sent yet: run ./demo.sh order, burst or return first"
    return 0
  fi
  read -r since sent kind < "$LAST_SENT"
  metadata_psql -q -c "
    SELECT a.name AS asset, to_char(e.timestamp, 'HH24:MI:SS') AS stored_at,
           coalesce('order ' || (e.extra::json->'payload'->>'order_id'),
                    'rows ' || (e.extra::json->'payload'->>'from_id') || ' to '
                            || (e.extra::json->'payload'->>'to_id')) AS payload
    FROM asset_event e JOIN asset a ON a.id = e.asset_id
    WHERE e.timestamp >= '$since'
    ORDER BY a.name, e.timestamp"
  echo "sent $sent at $(echo "$since" | cut -c12-19) UTC, Airflow stored $(stored_count) asset events"
  # Two watchers in one group (demo2-before) can take a minute to get through a burst: each
  # restarts after an event and the partition moves between them. Say so instead of looking final.
  if [[ $kind == kafka ]]; then
    lag=$(consumer_lag)
    if [[ -n $lag && $lag != 0 ]]; then
      echo "Kafka isn't done: $lag message(s) in orders not read yet. Run it again in a few seconds."
    fi
  fi
}

# A run takes the events stored up to the moment the first one queued its Dag. Events that land
# after that and before the scheduler creates the run aren't in it, and they wait for the next event
# (Airflow 3.3.2: apache/airflow#54659, fixed in 3.4 by #70972, and #56050). So a burst leaves some
# behind. reset drops them, or they'd show up in the first live run's log.
drop_leftover_events() {
  metadata_psql -At -c "
    WITH dropped AS (
      DELETE FROM asset_event e USING asset a
      WHERE a.id = e.asset_id AND a.name IN ('paid_orders', 'refunds', 'returns')
        AND e.timestamp < now() - interval '10 seconds'
        AND NOT EXISTS (SELECT 1 FROM dagrun_asset_event x WHERE x.event_id = e.id)
      RETURNING 1)
    SELECT count(*) FROM dropped"
}

# ---------------------------------------------------------------------------------------------
# Kafka consumer group

# Number of consumers in the group right now.
consumer_count() {
  kafka kafka-consumer-groups.sh --describe --group "$GROUP" --members 2>/dev/null |
    grep -c "^$GROUP " || true
}

# Messages in the orders topic that the group hasn't committed yet.
consumer_lag() {
  kafka kafka-consumer-groups.sh --describe --group "$GROUP" 2>/dev/null |
    awk -v g="$GROUP" '$1 == g && $2 == "orders" { print $6 }' | head -1
}

wait_for_consumers() {
  local start=$SECONDS
  until [[ $(consumer_count) == "$1" ]]; do
    if ((SECONDS - start > 120)); then
      echo "  expected $1 consumers in $GROUP, got $(consumer_count) after 2 minutes" >&2
      exit 1
    fi
    sleep 2
  done
}

# Waits until every message is committed, then for the Dag runs it started to finish.
wait_for_drain() {
  local start=$SECONDS
  until [[ $(consumer_lag) == "0" ]]; do
    if ((SECONDS - start > 180)); then
      echo "  messages still uncommitted after 3 minutes (lag $(consumer_lag))" >&2
      exit 1
    fi
    sleep 2
  done
  sleep 5
  for dag_id in ship_orders refund_orders; do
    wait_until_idle "$dag_id"
  done
}

# ---------------------------------------------------------------------------------------------
# Versions

# The Dag files a step needs, as "<folder>/<file>" under versions/. The third demo keeps the second
# demo's fixed Kafka Dags.
version_files() {
  case "$1" in
    demo1) echo "demo1/orders_kafka.py" ;;
    demo2-before) echo "demo2-before/orders_kafka.py" ;;
    demo2) echo "demo2/orders_kafka.py" ;;
    demo3-before) echo "demo2/orders_kafka.py demo3-before/returns_table.py" ;;
    demo3) echo "demo2/orders_kafka.py demo3/returns_table.py" ;;
    *) return 1 ;;
  esac
}

use_version() {
  local files wanted f
  if ! files=$(version_files "$1"); then
    echo "unknown step '$1': use demo1, demo2-before, demo2, demo3-before or demo3" >&2
    exit 1
  fi
  # Copy first, then remove what this step doesn't need, so a Dag that stays never disappears.
  wanted=""
  for f in $files; do
    cp "versions/$f" dags/
    wanted="$wanted $(basename "$f")"
  done
  for f in $DEMO_DAG_FILES; do
    case " $wanted " in
      *" $f "*) ;;
      *) rm -f "dags/$f" ;;
    esac
  done
  echo "active Dag files:$wanted (versions/$1)"
}

# Names of the assets that have a watcher, sorted, on one line.
watched_assets() {
  api "/assets?limit=100" | py '
import json, sys
print(" ".join(sorted(a["name"] for a in json.load(sys.stdin)["assets"] if a["watchers"])))'
}

# A watcher whose asset no Dag uses anymore keeps running until Airflow cleans it up, which took
# about two minutes in testing. Going back to demo1 waits for that, so it starts with one consumer.
wait_for_watchers() {
  local start=$SECONDS
  until [[ $(watched_assets) == "$1" ]]; do
    if ((SECONDS - start > 300)); then
      echo "  watched assets are '$(watched_assets)', expected '$1' after 5 minutes" >&2
      exit 1
    fi
    sleep 5
  done
}

reset_demo() {
  local id
  use_version demo1
  shop_psql -q -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/init.sql >/dev/null
  echo "returns table reset: 2 rows"
  if id=$(asset_id returns 2>/dev/null); then
    api "/assets/$id/state-store" DELETE
    echo "asset state store of 'returns' cleared"
  fi
  kafka kafka-topics.sh --delete --if-exists --topic orders_dlq >/dev/null 2>&1 || true
  # Topic deletion is asynchronous: retry the create until the old topic is gone.
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if kafka kafka-topics.sh --create --if-not-exists --topic orders_dlq --partitions 1 \
      --replication-factor 1 >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  echo "dead-letter topic emptied"
  rm -f "$LAST_SENT"
  echo "waiting until only demo1's watcher runs (up to a few minutes after the other demos)..."
  wait_for_watchers "paid_orders"
  wait_for_consumers 1
  echo "asset events no run took, dropped: $(drop_leftover_events)"
  echo "ready: demo1, one watcher, one consumer"
}

# ---------------------------------------------------------------------------------------------
# Dag runs

# Prints "<number of runs> <state of the newest run> <number of queued or running runs>".
run_summary() {
  # A Dag that doesn't exist (yet) counts as no runs.
  { airflow_cli dags list-runs "$1" -o json || true; } | py '
import json, sys
text = sys.stdin.read().strip()
runs = sorted(json.loads(text) if text.startswith("[") else [], key=lambda r: r["run_after"])
active = sum(r["state"] in ("queued", "running") for r in runs)
print(len(runs), runs[-1]["state"] if runs else "none", active)'
}

run_count() {
  run_summary "$1" | cut -d' ' -f1
}

# Waits until the Dag has more than <count> runs and none of them is queued or running.
wait_for_run() {
  local dag_id=$1 count=$2 start=$SECONDS summary
  while :; do
    summary=$(run_summary "$dag_id")
    set -- $summary
    if (($1 > count && $3 == 0)); then
      echo "  $dag_id: $2"
      return 0
    fi
    if ((SECONDS - start > 180)); then
      echo "  $dag_id: no finished run after 3 minutes" >&2
      exit 1
    fi
    sleep 3
  done
}

wait_until_idle() {
  wait_for_run "$1" -1 >/dev/null
}

# Restarts the triggerer, then prints the returns watcher's first poll after it: the cursor it starts
# from. That line is logged only once, then every poll repeats the new cursor, and a "trigger-logs"
# started before the restart stops with the container. So it would be gone by the time you look.
restart_triggerer() {
  local start=$SECONDS name started line
  name=$(container triggerer)
  docker restart "$name" >/dev/null
  echo "triggerer restarted in $((SECONDS - start))s"
  # The container's start time and its log timestamps both come from Docker, so --since skips every
  # line from before the restart, whatever the host clock says.
  started=$(docker inspect -f '{{.State.StartedAt}}' "$name")
  while ((SECONDS - start < 60)); do
    line=$(docker logs --since "$started" "$name" 2>&1 | grep -o -m1 'Checking returns after id [0-9]*' || true)
    if [[ -n $line ]]; then
      echo "first poll after the restart: $line"
      return 0
    fi
    sleep 1
  done
  echo "no 'Checking returns' line in the triggerer log after 60s: is a returns_table version active?"
}

# Task attempts that failed since $1 (UTC).
failed_attempts() {
  metadata_psql -At -F ' ' -c "
    SELECT dag_id, task_id, try_number, state FROM (
      SELECT dag_id, task_id, try_number, state, start_date FROM task_instance_history
      UNION ALL
      SELECT dag_id, task_id, try_number, state, start_date FROM task_instance
    ) AS attempts
    WHERE start_date >= '$1' AND state = 'failed'
    ORDER BY start_date"
}

rehearse() {
  local start=$SECONDS started_at runs stored failures
  started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "== reset"
  reset_demo

  echo "== demo1: a paid order starts ship_orders, a refund starts nothing"
  runs=$(run_count ship_orders)
  send "$(order_json paid "$(new_order_id)")" >/dev/null
  wait_for_run ship_orders "$runs"
  send "$(order_json refunded "$(new_order_id)")" >/dev/null
  wait_for_drain
  if [[ $(stored_count) != 0 ]]; then
    echo "  unexpected: the refund was stored as an asset event" >&2
    exit 1
  fi
  echo "  refund: no asset event, no run"

  echo "== demo2-before: a second watcher on the same topic drops events"
  use_version demo2-before >/dev/null
  wait_for_watchers "paid_orders refunds"
  wait_for_consumers 2
  burst >/dev/null
  wait_for_drain
  stored=$(stored_count)
  echo "  Airflow stored $stored of 6 events ($(stored_count paid_orders) paid, $(stored_count refunds) refunds)"
  if ((stored == 6)); then
    echo "  nothing was dropped this time (it depends on which watcher holds the partition)."
    echo "  Live, send ./demo.sh burst again: every burst in testing lost at least one event."
  fi

  echo "== demo2: one shared consumer, nothing dropped"
  use_version demo2 >/dev/null
  sleep 15 # the old triggers stop and the new ones subscribe
  wait_for_consumers 1
  sleep 10 # the cohort grace period, then the first poll
  burst >/dev/null
  wait_for_drain
  echo "  Airflow stored $(stored_count) of 6 events ($(stored_count paid_orders) paid, $(stored_count refunds) refunds)"
  send 'order 1234 paid, no JSON here' >/dev/null
  wait_for_drain
  echo "  bad message in the dead-letter topic: $(kafka kafka-console-consumer.sh --topic orders_dlq \
    --from-beginning --timeout-ms 4000 2>/dev/null | grep -c 'no JSON' || true)"

  echo "== demo3-before: the cursor in memory, lost on restart"
  use_version demo3-before >/dev/null
  runs=$(run_count restock_returns)
  wait_for_run restock_returns "$runs" # first start: the two rows already there
  runs=$(run_count restock_returns)
  add_return 4101 SNEAKERS-WHITE-42 >/dev/null
  wait_for_run restock_returns "$runs"
  runs=$(run_count restock_returns)
  restart_triggerer | sed 's/^/  /'
  wait_for_run restock_returns "$runs" # every row again
  echo "  after the restart, rows $(metadata_psql -At -c "SELECT e.extra::json->'payload'->>'from_id' || ' to ' || (e.extra::json->'payload'->>'to_id') FROM asset_event e JOIN asset a ON a.id = e.asset_id WHERE a.name = 'returns' ORDER BY e.id DESC LIMIT 1") were reported again"

  echo "== demo3: the cursor in the asset state store, kept across restarts"
  use_version demo3 >/dev/null
  runs=$(run_count restock_returns)
  wait_for_run restock_returns "$runs" # first start with an empty store: rows 1 to 3
  runs=$(run_count restock_returns)
  add_return 4102 HOODIE-GREY-L >/dev/null
  wait_for_run restock_returns "$runs"
  runs=$(run_count restock_returns)
  restart_triggerer | sed 's/^/  /'
  sleep 30
  if [[ $(run_count restock_returns) != "$runs" ]]; then
    echo "  unexpected: restock_returns ran again after the restart" >&2
    exit 1
  fi
  echo "  no run after the restart, cursor: $(api "/assets/$(asset_id returns)/state-store" | py '
import json, sys
print({e["key"]: e["value"] for e in json.load(sys.stdin)["asset_state_store"]})')"

  echo "== back to the starting point"
  reset_demo
  echo "rehearsal done in $(((SECONDS - start) / 60))m$(((SECONDS - start) % 60))s"
  failures=$(failed_attempts "$started_at")
  if [[ -n $failures ]]; then
    echo "WARNING: these task attempts failed (dag_id task_id try state). Check their logs:" >&2
    echo "$failures" >&2
    return 1
  fi
  echo "no failed task attempts"
}

# ---------------------------------------------------------------------------------------------

case "${1:-}" in
  rehearse)
    rehearse
    ;;
  reset)
    reset_demo
    ;;
  use)
    use_version "${2:-}"
    ;;
  order)
    send "$(order_json "${2:?usage: ./demo.sh order <paid|refunded> [order_id]}" "${3:-$(new_order_id)}")"
    ;;
  burst)
    burst
    ;;
  bad-message)
    send 'order 1234 paid, no JSON here'
    ;;
  received)
    received
    ;;
  consumers)
    kafka kafka-consumer-groups.sh --describe --group "$GROUP" --members 2>/dev/null || true
    kafka kafka-consumer-groups.sh --describe --group "$GROUP" 2>/dev/null || true
    ;;
  dlq)
    kafka kafka-console-consumer.sh --topic orders_dlq --from-beginning --timeout-ms 4000 2>/dev/null || true
    ;;
  return)
    add_return "${2:-}" "${3:-}"
    ;;
  returns)
    shop_psql -c "SELECT id, order_id, sku, reason, to_char(created_at, 'HH24:MI:SS') AS created FROM returns ORDER BY id"
    ;;
  cursor)
    api "/assets/$(asset_id returns)/state-store" | py '
import json, sys
entries = json.load(sys.stdin)["asset_state_store"]
for e in entries:
    print("%s = %s  (written by the %s at %s UTC)"
          % (e["key"], e["value"], e["last_updated_by"]["kind"], e["updated_at"][11:19]))
if not entries:
    print("empty: the watcher has not saved anything yet")'
    ;;
  restart-triggerer)
    restart_triggerer
    ;;
  trigger-logs)
    docker logs -f --tail 30 "$(container triggerer)"
    ;;
  wipe-runs)
    use_version demo1
    wait_for_watchers "paid_orders"
    for dag_id in $DEMO_DAG_IDS; do
      # Fails when the Dag doesn't exist, or while one of its tasks is running.
      if output=$(docker exec -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR "$(container scheduler)" \
        airflow dags delete "$dag_id" --yes 2>&1); then
        echo "deleted history of $dag_id"
      else
        echo "kept $dag_id: $(echo "$output" | grep -v '^[[:space:]]*$' | tail -1)"
      fi
    done
    ;;
  *)
    usage
    exit 1
    ;;
esac
