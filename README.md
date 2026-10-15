# ETL/ELT demo (Airflow 3.3)

Astro project for the "ETL/ELT in 2026" webinar. One orders pipeline grows in five steps, each
adding one Airflow feature.

Stack: Astro Runtime 3.3-8 (Airflow 3.3.2), MinIO for object storage, Postgres as the warehouse,
a fake exchange-rate API, and an OpenAI model for the agent in v5.

## What's in here

| Path | Used in | Purpose |
|---|---|---|
| `versions/v1/orders_pipeline.py` | v1 | Hardcoded store list, pandas, pure ETL |
| `versions/v2/orders_pipeline.py` | v2, v3 | Dynamic task mapping. v3 runs this same file |
| `versions/v3/xcom_backend.env` | v3, v4, v5 | The XCom backend settings, copied into `.env` by `./demo.sh use v3` |
| `versions/v4/ingest_orders.py` | v4, v5 | Ingest layer: small "t" before loading, retry policy, produces `raw_orders` |
| `versions/v4/transform_revenue.py` | v4, v5 | SQL in the warehouse, write-audit-publish, produces `daily_revenue` |
| `versions/v5/revenue_agent.py` | v5 | `@task.agent` with a read-only `SQLToolset`, triggered by `daily_revenue` |
| `versions/v5/revenue_agent_cryptic.py` | v5, optional | The same agent against cryptic column names |
| `include/exchange_rates.py` | all | API client that raises `RateLimitedError` (429) or `ForbiddenError` (403) |
| `include/demo_tools/generate_orders.py` | `demo.sh` | Writes the store files to MinIO |
| `warehouse/init.sql` | all | Tables, 13 days of history, the agent's read-only role |
| `fake_api/server.py` | all | Exchange-rate API with three modes: `ok`, `flaky`, `bad-key` |
| `docker-compose.override.yml` | | `minio`, `minio-init`, `warehouse`, `fake-api`, and the LLM key passthrough |
| `airflow_settings.yaml` | | Connections: `minio`, `warehouse`, `warehouse_agent`, `exchange_rates_api`, `llm` |
| `.env` | | Demo settings. `demo.sh` fills or empties its XCom backend block |
| `demo.sh` | | Helper commands for the demo (`./demo.sh` lists them) |

Each `versions/` folder holds only what its version adds. v3 adds configuration and no Dag code,
and v5 adds two Dags next to the v4 ones. `dags/` only holds the active version's files:
`./demo.sh use <version>` copies them from `versions/`. Edit the files in `versions/`, never in
`dags/`: the next `use` or `reset` overwrites them.

## Requirements

- Astro CLI (tested with 1.46) and Docker.
- Run only this Astro project, and turn off Docker Desktop's Kubernetes if you don't need it. This
  project idles at about 2 GB and peaked at about 3 GB during a clean rehearsal. With a second
  Astro project and Kubernetes on an 8 GB Docker VM, the big file pushed the VM out of memory
  twice in one rehearsal (v2, and v4's 429 run): the kernel killed Docker's DNS, and tasks failed
  with `Temporary failure in name resolution`. In v4, Airflow retried the lost tasks about 3
  minutes later and the run still ended in `success`, which is why `rehearse` now ends with a
  check for failed task attempts. With only this project running (Kubernetes still on), the
  rehearsal had no failures and took half the time.
- `OPENAI_API_KEY` exported in the shell that runs `astro dev start` (only v5 needs it). The key is
  passed through by `docker-compose.override.yml` and never written to a file.
- macOS (the helper script uses the bash 3.2 that ships with it).

## Start

```bash
astro dev start
./demo.sh reset
```

| What | Where | Login |
|---|---|---|
| Airflow UI | http://localhost:8081, or the proxy URL that `astro dev start` prints | none |
| MinIO console | http://localhost:9001 | `minio-admin` / `minio-demo-2026` |
| Warehouse | `localhost:5436`, database `warehouse` | `etl` / `wh-demo-2026` |
| Fake API | http://localhost:8090/rates | none |

The ports avoid the defaults on purpose (8081 and 5435 are set in `.astro/config.yaml`), so this
project can run next to another one when you're not demoing.

## Before going live (webinar day)

1. Stop any other Astro project (`astro dev stop` in its folder) and Docker Desktop's Kubernetes.
2. Run `./demo.sh wipe-runs`, then `./demo.sh rehearse`. Wiping first keeps the history clean
   (only the rehearsal's runs). The rehearsal plays the whole demo unattended in about 6
   minutes, for less than 2 cents of LLM calls. Every version then has finished runs to show,
   including the 429 retry and the 403 fast failure. It ends with `./demo.sh reset`, so you start
   from v1. Check that every step printed `success`, except the 403 step, which must say `failed`,
   and that the last line is `no unexpected task failures`. If it lists failed attempts instead,
   free memory for Docker and do step 2 again.
3. Do step 2 on the webinar day itself (same UTC day). A Dag with `schedule="@daily"` starts one
   run by itself the first time Airflow sees it each day, even with `catchup=False`. After the
   rehearsal those runs have already happened, so nothing starts on its own during the demo.
4. Open the tabs: Airflow UI (Dags list), MinIO console (bucket `xcom`).
5. Record a backup video of the full demo.

## Follow the demo

Trigger Dags with the UI's Trigger button, or `./demo.sh trigger <dag_id>`.

### v1: the version everyone writes first

1. Tour the UI on `orders_pipeline`: Dags list, Grid, Graph, a task's logs, the daily schedule.
2. Trigger it. `./demo.sh revenue` then shows four stores for 2026-10-13 (the reset left only the
   13 days of history before it).
3. `./demo.sh new-store`: `store_toronto.csv` lands in the bucket. Trigger again, then
   `./demo.sh revenue`: still four stores. v1 is blind to new sources.

### v2: dynamic task mapping

1. `./demo.sh use v2`, wait about 10 seconds. The UI shows a new Dag version. Its number depends
   on the history: after `wipe-runs` and `rehearse` it's version 4, because every switch back to
   v1 also counts as a new version.
2. Trigger: one `extract` task per file, named after its store, at most 4 running at once. Each
   takes about a second, so show the cap in the run's Gantt view: the fifth task starts when one
   of the first four finishes.
3. `./demo.sh revenue`: Toronto is in, with no code change.
4. `./demo.sh big-file`: the online store sends 400,000 orders (36 MB of CSV). Trigger. It still
   works: Airflow stores DataFrames as compressed Parquet. The point is where the data went:
   `./demo.sh xcom-size` shows a 10 MB XCom for `extract` / `online`, the orders themselves, in
   Airflow's metadata database. Don't open that task's XCom tab (Storage, then XCom) live: it
   freezes the page for 5 to 10 seconds, and the value doesn't even show, because it's a single
   line of hex millions of pixels wide.

### v3: custom XCom backend

1. `./demo.sh use v3` copies the XCom backend settings from `versions/v3/xcom_backend.env` into
   `.env` and restarts Airflow (about 20 seconds, while you explain the config).
2. Trigger. `./demo.sh xcom-size`: the online store's value is now
   `"s3://minio@xcom/xcom/..."`, a reference. The MinIO console shows the object (about 2.7 MB,
   gzipped) in the `xcom` bucket. Values under 1 MB stay in the database.

### v4: split by layer, connected by assets

1. `./demo.sh use v4`: `orders_pipeline` disappears, `ingest_orders` and `transform_revenue`
   appear.
2. Trigger `ingest_orders` only. `transform_revenue` starts by itself when ingest finishes
   (run id `asset_triggered__...`). Show the asset graph.
3. The retry policy, from the rehearsal's finished runs of `ingest_orders`:
   - `get_exchange_rates` with three attempts: two 429s, each retried after 5 seconds, then
     success. The log says `Retry policy decision action=retry reason='Rate limited: worth another try'`.
   - `get_exchange_rates` failed on attempt 1 of 5: `action=fail reason='API key rejected: retrying will not help'`.
   - To show it live instead: `./demo.sh api-mode flaky`, trigger (about 20 seconds), then
     `./demo.sh api-mode bad-key`, trigger, and `./demo.sh api-mode ok` afterwards.

### v5: an agent consumes the same asset

1. `./demo.sh use v5`: `revenue_agent` and `revenue_agent_cryptic` appear.
2. Open the rehearsal's `revenue_agent` run: its XCom (Storage, then XCom) holds the structured
   answer (`summary`, `anomalies`). The log shows each SQL query the agent ran
   (`Running statement: ...`), then `LLM run complete` with token counts, and `LLM run cost`.
3. Live, if time allows: trigger `ingest_orders`. Ingest, transform and agent run one after the
   other, chained by assets (about 45 seconds).
4. Optional: compare with the answer of `revenue_agent_cryptic`.

## Other commands

| Command | What it does |
|---|---|
| `./demo.sh files` | List the store files in the landing bucket |
| `./demo.sh psql` | Interactive psql on the warehouse |
| `./demo.sh logs` | Follow the scheduler logs (tasks run in the scheduler container) |
| `./demo.sh wipe-runs` | Switch to v1, then delete the run history and Dag versions of the demo Dags. `orders_pipeline` starts its daily run again right away, and the v4 Dags do the same when they next appear. A Dag with a running task is kept, and the command says so: wait, then run it again. Run `./demo.sh rehearse` afterwards. |
| `astro dev kill` | Remove all containers and volumes (MinIO and warehouse data included) |

## Use another LLM provider

Change `model` in the `llm` connection in `airflow_settings.yaml` (for example
`anthropic:<model id>`), add that provider's key variable next to `OPENAI_API_KEY` in
`docker-compose.override.yml`, add the provider extra in `requirements.txt` (for example
`apache-airflow-providers-common-ai[anthropic,sql]`), then `astro dev restart`.

## Things learned while building (good Q&A material)

- **`@daily` Dags run once on first sight.** With `catchup=False`, Airflow still runs the latest
  interval when it first sees the Dag. That's why the rehearsal must happen on the day.
- **Cron schedules have zero-length data intervals.** In Airflow 3, `@daily` uses
  `CronTriggerTimetable`: a run's `data_interval_start` equals its `data_interval_end`. Don't
  build incremental loads on the data interval unless you switch to `CronDataIntervalTimetable`.
- **psycopg 3.** The postgres provider (7.1 here) switches to psycopg 3 when SQLAlchemy 2 is
  installed, as in Astro Runtime 3.3. Bulk loads use `cursor.copy()`, not psycopg2's
  `copy_expert()`.
- **XCom backend compression.** `xcom_objectstorage_compression` takes the codec name (`gzip`). The
  file suffix (`gz`), as in the provider's config example, raises an error.
- **DataFrames in XCom are compact.** Airflow stores them as snappy-compressed Parquet, written
  as hex text: 400,000 orders take 10 MB. Big XComs rarely crash; they quietly fill the metadata
  database.
- **Single-file bind mounts go stale.** Docker Desktop kept serving the old length of an edited
  `init.sql`, which silently cut off its last statements. The compose file mounts folders only.
- **Masked passwords.** Airflow hides connection passwords in every log line, so the demo
  passwords are not words that appear in logs.
- **The agent sees names and types, not comments.** `SQLToolset` reads column names and types, so
  column descriptions go in the system prompt.
- **`allowed_tables` is enforced, but it isn't the boundary.** `SQLToolset` parses every query and
  rejects tables outside the list. Its own docs call that a guardrail, not a replacement for
  database permissions, so `warehouse_agent` uses a role that can only read `daily_revenue` and
  `rev_x`.
