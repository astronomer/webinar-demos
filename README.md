# Debugging Airflow's scariest issues

Companion code for the Astronomer webinar **Debugging Airflow's scariest issues** from 2026-10-29. [TODO: recording link to be added by Sable]

Most Airflow errors are not scary: an import error or a task log with a traceback has everything you, or your AI agent, need to fix it. This repo contains the ones where the error message is missing or points in the wrong direction. Each one is a "house" in the webinar, and each house answers two questions: what could cause this symptom, and what context would an AI agent need to find the cause?

| House | Debugging ladder rung | Symptom | Cause in this demo | Folder |
|---|---|---|---|---|
| Blue | Infrastructure | A failed task with no logs | The worker ran out of ephemeral storage and the pod was evicted | `dags/house_1_blue/` |
| Green | Dag parsing | A Dag disappears, and there is no import error | `dagbag_import_timeout` is larger than `dag_file_processor_timeout` | `dags/house_2_green/` |
| Yellow | Dag and task runs | An extra Dag run, or a missing hour of data | The daylight saving time change in local-time schedules and partition keys | `dags/house_3_yellow/` |
| Red | Task logic | Customers got charged twice | A task retry relaunched a remote job that was still running | `dags/house_4_red/` |
| Prevention | Prevention | Everything is red, and the UI returns 500 | A cloud region outage. The fix is disaster recovery, so this house has no Dag | none |

All versions below are the ones the demos were tested with: Astro Runtime `3.3-11` (Airflow 3.3), `apache-airflow-providers-ssh==7.0.0`, the local Astro CLI environment with the LocalExecutor. House 1 needs an Astro Deployment.

## Run it locally

1. Install the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) and Docker.
2. Create a `.env` file in the project root with the SSH connection that house 4 uses:

   ```text
   AIRFLOW_CONN_SSH_DEFAULT={"conn_type":"ssh","host":"ssh-target","port":2222,"login":"airflow","password":"airflow","extra":{"no_host_key_check":true}}
   ```

   The credentials belong to a throwaway local container. `no_host_key_check` turns off host key verification, which is fine for this demo and not for a real host.

3. Run `astro dev start`.

`docker-compose.override.yml` adds an `ssh-target` container (`lscr.io/linuxserver/openssh-server`), which plays the remote billing host for house 4. The `Dockerfile` installs the SSH provider with `pip install --uploaded-prior-to`, so transitive dependencies resolve to versions that were published before that date too. It also installs the Amazon provider, which is only needed for the optional remote logging experiment in house 1.

## House 1 (blue): a failed task with no logs

Dags: `resource_stress_test_no_queues` and `resource_stress_test_with_queues`.

`high_storage` writes 50 GB to `/tmp`. On an Astro Deployment with a small default worker, the worker pod exceeds its ephemeral storage limit, Kubernetes evicts it, and the task fails without a task log. The `with_queues` version sends the same task to a worker queue with enough storage, so it succeeds. It expects worker queues named `cpu-intensive`, `memory-intensive`, and `storage-intensive`.

Don't run `high_storage` locally. It fills the disk of your Docker VM, and there is no pod to evict, so it doesn't reproduce the symptom.

Other causes of a failed task without logs that the webinar covers:

- **The task never started running.** It was marked failed by hand (check the audit log), or it hit `[scheduler] task_queued_timeout` while queued. The scheduler requeues a task that is stuck in queued a limited number of times, then fails it and writes "stuck in queued tries exceeded" to the audit log.
- **The logs exist, but you can't see them.** Fetching logs from the worker timed out (`[api] log_fetch_timeout_sec`, default 5 seconds), or remote logging is misconfigured on the read side. Both errors only show up in the API server logs.
- **Remote logging is misconfigured on the write side.** The task ran, but its log was never uploaded.

To reproduce the remote logging read failure locally, point remote logging at an endpoint that doesn't exist by adding this to `.env` and restarting:

```text
AIRFLOW__LOGGING__REMOTE_LOGGING=True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://no-logs-test-bucket/logs
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_logs_broken
AIRFLOW_CONN_AWS_LOGS_BROKEN={"conn_type":"aws","login":"dummy","password":"dummy","extra":{"endpoint_url":"http://127.0.0.1:1","region_name":"us-east-1"}}
AWS_MAX_ATTEMPTS=1
```

Then run any task. In Airflow 3.3, the log pane in the UI stays empty, while the REST API returns a 500 for the same log.

## House 2 (green): the Dag that disappears without an import error

Dag: `customer_reports`.

The Dag file has slow top-level code that only runs when a toggle file exists:

```sh
touch include/house_2_green/slow_parse_on   # parsing now takes 35 seconds
rm include/house_2_green/slow_parse_on      # back to normal
```

Two timeouts apply to parsing:

| Config | Default | Scope |
|---|---|---|
| `[core] dagbag_import_timeout` | 30.0 | Importing one Dag file. Raises an import error that shows up in the UI. |
| `[dag_processor] dag_file_processor_timeout` | 50 | The whole parsing process for one file. The Dag processor kills the process, and no import error is written. |

With the defaults, the 35 second parse hits `dagbag_import_timeout` first and you get a readable import error. Now raise `dagbag_import_timeout` above `dag_file_processor_timeout`, for example because someone fixed slow parsing that way:

```text
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120
```

Now the Dag processor kills the parsing process at 50 seconds. The only trace is a line in the Dag processor logs, there is no import error, and the Dag is marked stale after `[dag_processor] stale_dag_threshold` (default 50 seconds) and disappears from the Dags list. To see this faster in a local demo, lower `AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT` to 20 instead.

The fix is to keep `dagbag_import_timeout` lower than `dag_file_processor_timeout`, and to make the top-level code fast.

## House 3 (yellow): daylight saving time

On 2026-10-25, Berlin turns its clocks back from 03:00 CEST to 02:00 CET, so the local hour 02:00 happens twice. All demo Dags cover that night with `catchup=True` and an `end_date`, so they create their runs right away. They write to a SQLite file at `/tmp/house_3_yellow.db` inside the container (`include/house_3_yellow/store.py`). The order amount for each hour is `1000 + 37 * utc_hour`, so you can tell the hours apart.

### Two runs, silent data loss

- `hourly_revenue_snapshot`: a plain `0 * * * *` cron schedule in Europe/Berlin. Airflow runs it at both 02:00 hours. The task stores revenue per local hour formatted as `%Y-%m-%d %H:00`, so the second 02:00 overwrites the first, and EUR 1,000.00 of revenue disappears without an error.
- `hourly_revenue_snapshot_with_offset`: the same Dag with `%z` in the key (`02:00+0200` and `02:00+0100`), so both hours survive.

The `*_slide.py` files are the same two Dags without the demo-only settings (`catchup`, `end_date`, `max_active_runs`), for the slides.

In spring the opposite happens: on 2027-03-28, Berlin skips from 02:00 to 03:00, so an hourly local-time schedule has no 02:00 run.

### Partitioned assets

- `berlin_hourly_orders` uses `CronPartitionTimetable` in Europe/Berlin. The default partition key format, `%Y-%m-%dT%H:%M:%S`, has no UTC offset, so both 02:00 runs produce the same partition key.
- `eu_hourly_revenue` consumes those partitions with `StartOfHourMapper`, gets a second Dag run for the duplicate key, and both hours land in the same revenue bucket.
- `us_hourly_revenue` converts Berlin time to New York time with a static -6 hours. That's wrong between 2026-10-25 and 2026-11-01, because Europe and the US don't change their clocks on the same day.

The `*_with_offset` versions add `%z` to the key format and to the mapper's input and output formats, and the US Dag uses a mapper with the `America/New_York` time zone instead of a static offset. The general fix: convert to UTC as early as possible, and keep the offset in any local time you store.

## House 4 (red): we charged people twice

Dags: `send_invoices` and `send_invoices_task_state_store`.

The task launches `/opt/jobs/send_invoices.sh` on the SSH target in the background with `nohup`, then opens a second SSH session that waits for the job to finish. The job waits 45 seconds, then "sends" five invoices by appending them to `/config/outbox/<run_id>.log` on the remote host.

To reproduce the double billing:

1. In one terminal, watch the remote jobs: `sh include/house_4_red/remote_host.sh ps`
2. Trigger `send_invoices`.
3. In another terminal, run `sh include/house_4_red/drop_ssh_check_session.sh`. It waits for the waiting session, then kills its `sshd` process, the way a network blip or a proxy timeout would.
4. The task fails with a `ConnectionError` and retries. The retry launches the job a second time, while the first one is still running on the remote host.
5. After both jobs finish, run `sh include/house_4_red/remote_host.sh out`. Every customer got two invoices.

The Airflow task log only shows a dropped connection and a successful retry. The duplicate only shows up in the destination system.

`send_invoices_task_state_store` stores the remote PID in the `task_state_store`. It is scoped to the task instance and survives retries. The retry reads the PID and reconnects to the running job instead of launching a new one. Run the same steps against it and each customer gets one invoice.

`appendix_task_state_store_ssh_variants.py` (Dag `task_state_store_ssh_variants`) shows the same pattern with a `PythonOperator`, a custom `BaseOperator`, and a subclassed `SSHOperator`, which access the store through `context["task_state_store"]`.

## Other files

- `dags/other/missing_connection.py`: a task that fails with a clear, not-scary error in the task log.
- `dags/other/resource_metrics_demo.py`: synthetic CPU and memory profiles for the Astro task-level resource metrics view. The docstring has the details.
- `plugins/task_memory_profiler/`: a prototype plugin that adds a Memory tab with a memray flamegraph to task instances of Dags tagged `memory-profile`. It needs `memray` in `requirements.txt`, which this project doesn't install. See its own README.

## Resources

- [Task state store](https://www.astronomer.io/docs/airflow/airflow-task-state-store)
- [Debug Dags with AI](https://www.astronomer.io/docs/airflow/debug-dags-with-ai)
- [Airflow configuration reference](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)
- Airflow skills for your AI agent: `npx skills add astronomer/agents --skill '*'`
