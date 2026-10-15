#!/usr/bin/env bash
# Helper commands for the ETL/ELT webinar demo. Run from anywhere; it works in this folder.
# Works with the bash 3.2 that ships with macOS.
set -euo pipefail

cd "$(dirname "$0")"

DEMO_DAG_FILES="orders_pipeline.py ingest_orders.py transform_revenue.py revenue_agent.py revenue_agent_cryptic.py"
DEMO_DAG_IDS="orders_pipeline ingest_orders transform_revenue revenue_agent revenue_agent_cryptic"

usage() {
  cat <<EOF
usage: ./demo.sh <command>

  rehearse                    run the whole demo once, unattended (about 6 minutes), so every
                              version has finished runs to show; ends with a reset and a check
                              for failed task attempts
  reset                       back to the starting point: four store files, fresh warehouse,
                              API mode ok, v1 active (keeps Dag run history)
  use <v1|v2|v3|v4|v5>        activate one pipeline version (v3 to v5 turn the XCom backend on,
                              which restarts Airflow when it changes)
  new-store                   a new store's file lands in the bucket
  big-file                    the online store sends a much bigger file
  files                       list the files in the landing bucket
  xcom-size                   how much XCom data sits in Airflow's metadata database, and the
                              biggest values (v2: the orders themselves, v3: a reference)
  api-mode <ok|flaky|bad-key> switch the fake exchange-rate API (flaky: next two calls get 429)
  trigger <dag_id>            trigger a Dag run
  revenue                     show daily_revenue for the latest day
  psql                        interactive psql on the warehouse
  logs                        follow the scheduler logs (where tasks run)
  wipe-runs                   switch to v1, then delete the run history and Dag versions of the
                              demo Dags
EOF
}

container() {
  local name
  name=$(docker ps --format '{{.Names}}' | grep -E "^etl-elt-demo_.*-$1-1$" | head -1 || true)
  if [[ -z "$name" ]]; then
    echo "no running '$1' container (is 'astro dev start' running?)" >&2
    exit 1
  fi
  echo "$name"
}

airflow_python() {
  docker exec -w /usr/local/airflow "$(container scheduler)" python "$@"
}

warehouse_psql() {
  docker exec -i -e PGOPTIONS=--client-min-messages=warning "$(container warehouse)" psql -U etl -d warehouse "$@"
}

api_mode() {
  curl -fsS -X POST "http://127.0.0.1:8090/mode/$1"
  echo
}

XCOM_BACKEND_SETTINGS=versions/v3/xcom_backend.env

# Fills (on) or empties (off) the XCom backend block in .env with the v3 settings. Returns 0 when
# .env changed.
set_xcom_backend() {
  local before after
  before=$(cksum < .env)
  awk -v on="$1" -v settings="$XCOM_BACKEND_SETTINGS" '
    /^# >>> xcom-backend/ {
      print
      if (on == "on") while ((getline line < settings) > 0) print line
      skipping = 1
      next
    }
    /^# <<< xcom-backend/ { skipping = 0 }
    !skipping
  ' .env > .env.tmp
  mv .env.tmp .env
  after=$(cksum < .env)
  [[ $before != "$after" ]]
}

# The Dag files a version needs, as "<folder>/<file>" under versions/. Each folder holds only what
# its version adds: v3 adds configuration (versions/v3/xcom_backend.env), not Dag code.
version_files() {
  case "$1" in
    v1) echo "v1/orders_pipeline.py" ;;
    v2 | v3) echo "v2/orders_pipeline.py" ;;
    v4) echo "v4/ingest_orders.py v4/transform_revenue.py" ;;
    v5) echo "v4/ingest_orders.py v4/transform_revenue.py v5/revenue_agent.py v5/revenue_agent_cryptic.py" ;;
    *) return 1 ;;
  esac
}

use_version() {
  local files wanted f xcom
  if ! files=$(version_files "$1"); then
    echo "unknown version '$1': use v1, v2, v3, v4 or v5" >&2
    exit 1
  fi
  # Copy first, then remove what this version doesn't need, so a Dag that stays never disappears.
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
  echo "active Dag files:$wanted"

  case "$1" in
    v1 | v2) xcom=off ;;
    *) xcom=on ;;
  esac
  if set_xcom_backend "$xcom"; then
    echo "XCom backend switched $xcom, restarting Airflow..."
    local start=$SECONDS
    astro dev restart
    echo "restarted in $((SECONDS - start))s"
  fi
}

reset_demo() {
  airflow_python -m include.demo_tools.generate_orders reset
  warehouse_psql -q -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/init.sql >/dev/null
  echo "warehouse reset: 13 days of history for nyc, london, berlin, tokyo"
  api_mode ok
  use_version v1
}

airflow_cli() {
  docker exec -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR "$(container scheduler)" airflow "$@" 2>/dev/null
}

# Prints "<number of runs> <state of the newest run> <number of queued or running runs>".
run_summary() {
  airflow_cli dags list-runs "$1" -o json | docker exec -i "$(container scheduler)" python -c '
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
    if ((SECONDS - start > 600)); then
      echo "  $dag_id: no finished run after 10 minutes" >&2
      exit 1
    fi
    sleep 3
  done
}

wait_until_idle() {
  wait_for_run "$1" -1 >/dev/null
}

trigger_and_wait() {
  local dag_id=$1 count attempts=0
  wait_until_idle "$dag_id"
  count=$(run_count "$dag_id")
  # Right after a file swap or a restart the Dag may not be parsed yet.
  until airflow_cli dags trigger "$dag_id" >/dev/null; do
    attempts=$((attempts + 1))
    if ((attempts > 20)); then
      echo "  could not trigger $dag_id" >&2
      exit 1
    fi
    sleep 3
  done
  wait_for_run "$dag_id" "$count"
}

# Gives the Dag processor time to parse swapped files (refresh interval is 5 seconds).
wait_for_parse() {
  sleep 15
}

# Task attempts that failed since $1 (UTC), except get_exchange_rates, whose 429 and 403 failures
# are part of the demo. A run can still end in success after a failed attempt, because Airflow
# retries tasks it lost (for example when the Docker VM runs out of memory).
unexpected_failures() {
  docker exec "$(container postgres)" psql -U postgres -d postgres -At -F ' ' -c "
    SELECT dag_id, task_id, map_index, try_number, state FROM (
      SELECT dag_id, task_id, map_index, try_number, state, start_date FROM task_instance_history
      UNION ALL
      SELECT dag_id, task_id, map_index, try_number, state, start_date FROM task_instance
    ) AS attempts
    WHERE start_date >= '$1' AND state = 'failed' AND task_id <> 'get_exchange_rates'
    ORDER BY start_date"
}

rehearse() {
  local start=$SECONDS started_at transform_runs agent_runs failures
  started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "== reset"
  reset_demo
  wait_for_parse
  echo "== v1: four stores"
  trigger_and_wait orders_pipeline
  echo "== v1: a new store opens, and v1 ignores it"
  airflow_python -m include.demo_tools.generate_orders new-store
  trigger_and_wait orders_pipeline
  echo "== v2: dynamic task mapping picks up the new store"
  use_version v2
  wait_for_parse
  trigger_and_wait orders_pipeline
  echo "== v2: the big file goes through the metadata database"
  airflow_python -m include.demo_tools.generate_orders big-file
  trigger_and_wait orders_pipeline
  echo "== v3: same Dag with the XCom backend on"
  use_version v3
  wait_for_parse
  trigger_and_wait orders_pipeline
  echo "== v4: ingest, then the raw_orders asset starts the transform"
  use_version v4
  wait_for_parse
  # A Dag seen for the first time today starts its daily run by itself: let it finish first.
  wait_until_idle ingest_orders
  sleep 10
  wait_until_idle transform_revenue
  transform_runs=$(run_count transform_revenue)
  trigger_and_wait ingest_orders
  wait_for_run transform_revenue "$transform_runs"
  echo "== v4: rate limited (429), retried by the policy"
  api_mode flaky >/dev/null
  transform_runs=$(run_count transform_revenue)
  trigger_and_wait ingest_orders
  wait_for_run transform_revenue "$transform_runs"
  echo "== v4: API key rejected (403), fails on the first try"
  api_mode bad-key >/dev/null
  trigger_and_wait ingest_orders
  api_mode ok >/dev/null
  echo "== v5: ingest, transform, then the agent, all chained by assets"
  use_version v5
  wait_for_parse
  transform_runs=$(run_count transform_revenue)
  agent_runs=$(run_count revenue_agent)
  trigger_and_wait ingest_orders
  wait_for_run transform_revenue "$transform_runs"
  wait_for_run revenue_agent "$agent_runs"
  echo "== v5: the same agent against cryptic column names"
  trigger_and_wait revenue_agent_cryptic
  echo "== back to the starting point"
  reset_demo
  echo "rehearsal done in $(((SECONDS - start) / 60))m$(((SECONDS - start) % 60))s"
  failures=$(unexpected_failures "$started_at")
  if [[ -n $failures ]]; then
    echo "WARNING: these task attempts failed (dag_id task_id map_index try state). Check their" >&2
    echo "logs, free memory for Docker (see README), then run ./demo.sh wipe-runs and rehearse again:" >&2
    echo "$failures" >&2
    return 1
  fi
  echo "no unexpected task failures"
}

cmd=${1:-}
case "$cmd" in
  rehearse)
    rehearse
    ;;
  reset)
    reset_demo
    ;;
  use)
    use_version "${2:-}"
    ;;
  new-store)
    airflow_python -m include.demo_tools.generate_orders new-store
    ;;
  big-file)
    airflow_python -m include.demo_tools.generate_orders big-file
    ;;
  files)
    airflow_python -m include.demo_tools.generate_orders list
    ;;
  xcom-size)
    docker exec "$(container postgres)" psql -U postgres -d postgres \
      -c "SELECT pg_size_pretty(pg_total_relation_size('xcom')) AS xcom_table_size, count(*) AS xcom_values FROM xcom" \
      -c "SELECT x.dag_id, x.task_id, coalesce(t.rendered_map_index, x.map_index::text) AS mapped_task,
                 pg_size_pretty(length(x.value::text)::bigint) AS size, left(x.value::text, 60) AS starts_with
          FROM xcom x
          LEFT JOIN task_instance t
            ON t.dag_id = x.dag_id AND t.run_id = x.run_id AND t.task_id = x.task_id AND t.map_index = x.map_index
          WHERE x.run_id = (SELECT run_id FROM dag_run ORDER BY id DESC LIMIT 1)
          ORDER BY length(x.value::text) DESC LIMIT 10"
    ;;
  api-mode)
    api_mode "${2:-}"
    ;;
  trigger)
    airflow_cli dags trigger "${2:?usage: ./demo.sh trigger <dag_id>}"
    ;;
  revenue)
    warehouse_psql -c "SELECT * FROM daily_revenue WHERE order_date = (SELECT max(order_date) FROM daily_revenue) ORDER BY revenue_usd DESC"
    ;;
  psql)
    docker exec -it "$(container warehouse)" psql -U etl -d warehouse
    ;;
  logs)
    docker logs -f --tail 50 "$(container scheduler)"
    ;;
  wipe-runs)
    # A @daily Dag whose history is deleted starts a new run right away: switch to v1 first so
    # only orders_pipeline does, instead of the whole v4/v5 chain.
    use_version v1
    for dag_id in $DEMO_DAG_IDS; do
      # Fails when the Dag doesn't exist yet, or while one of its tasks is running.
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
