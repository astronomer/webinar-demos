# Event-driven scheduling demo (Airflow 3.3)

Astro project for the webinar
[Build event-driven workflows with Apache Airflow®](https://www.astronomer.io/events/webinars/build-event-driven-workflows-with-apache-airflow-video/)
(November 4, 2026). One online shop and three demos. The first starts a Dag from Kafka. The
second puts a second watcher on the same topic, which drops events until a shared stream fixes
it. The third watches a table, which needs a memory: lost on every restart until the asset state
store keeps it.

Stack: Astro Runtime 3.3-11 (Airflow 3.3.2), the apache-kafka provider 2.1.0, a single-node Kafka
3.9 (topics `orders` and `orders_dlq`) and a Postgres database `shop` with a `returns` table.

## What's in here

| Path | Used in | Purpose |
|---|---|---|
| `versions/demo1/orders_kafka.py` | First demo | `MessageQueueTrigger(scheme="kafka")` watching `orders` for paid orders, the `ship_orders` Dag. The SQS swap is in a comment |
| `versions/demo2-before/orders_kafka.py` | Second demo, before the fix | The first demo's Dag plus a second `MessageQueueTrigger` for refunds and the `refund_orders` Dag. Two consumers in one group: events get dropped |
| `versions/demo2/orders_kafka.py` | Second demo, after the fix; third demo | The same two watchers on one shared stream (`OrderEventsTrigger`) with a dead-letter topic |
| `versions/demo3-before/returns_table.py` | Third demo, before the fix | A watcher on the `returns` table with its cursor in memory, the `restock_returns` Dag |
| `versions/demo3/returns_table.py` | Third demo, after the fix | The same watcher with its cursor in the asset state store. The task reads it too |
| `include/filters.py` | First demo; second demo before the fix | `is_paid_order` and `is_refund`, the `apply_function`s of the Kafka triggers |
| `include/triggers/order_events.py` | Second demo, after the fix | `OrderEventsTrigger`: about 15 lines subclassing `KafkaSharedStreamTrigger`, to filter by event type and reject messages that aren't JSON |
| `include/triggers/returns.py` | Third demo | `InMemoryReturnsTrigger` (before the fix) and `StatefulReturnsTrigger` (after) |
| `shop_db/init.sql` | Third demo | The `returns` table with two rows. `./demo.sh reset` runs it again |
| `docker-compose.override.yml` | | `kafka`, `kafka-init` (creates both topics) and `shop-db` |
| `airflow_settings.yaml` | | Connections `kafka_default` and `shop_db` |
| `.env` | | Demo settings: Dags unpaused, fast Dag parsing, the shared-stream grace period |
| `demo.sh` | | Helper commands for the demo (`./demo.sh` lists them) |

`dags/` only holds the active step's files: `./demo.sh use <step>` copies them from `versions/`.
There is one folder per step: `demo1`, then `demo2-before` and `demo2` (the second demo before and
after the fix), then `demo3-before` and `demo3`. The third demo's steps keep `demo2/orders_kafka.py`
next to the returns Dag, so the order watchers keep running through the end of the demo. Edit the
files in `versions/`, never in `dags/`: the next `use` or `reset` overwrites them. The triggers live
in `include/` because the triggerer imports a trigger by its module path.

## Requirements

- Astro CLI (tested with 1.46.0) and Docker.
- About 2.6 GB of Docker memory while idle (the scheduler about 1 GB, Kafka, the triggerer and the
  API server about 0.45 GB each). The rehearsal passed on an 8 GB Docker VM with another Airflow
  stack (`af34`, about 1.5 GB) running next to it. On the webinar day, run only this project.
- macOS (the helper script uses the bash 3.2 that ships with it).
- No API keys and no cloud account: everything runs locally.

## Start

```bash
astro dev start
./demo.sh reset
```

| What | Where | Login |
|---|---|---|
| Airflow UI | http://localhost:8082, or the proxy URL that `astro dev start` prints | none |
| Airflow REST API | http://localhost:8082/api/v2 | none (no token needed locally) |
| Shop database | `localhost:5438`, database `shop` | `shop` / `pg-demo-2026` |
| Kafka | `kafka:9092`, inside Docker only | use `./demo.sh` |

The ports avoid the defaults on purpose (8082 and 5437 are set in `.astro/config.yaml`), so this
project can run next to the ETL project (8081) or another stack on 8080 when you're not demoing.

## Before going live (webinar day)

1. Stop every other Astro project (`astro dev stop` in its folder) and any other Airflow
   containers (`docker ps` lists them).
2. Run `./demo.sh wipe-runs`, then `./demo.sh rehearse`. Wiping first keeps the history clean
   (only the rehearsal's runs). The rehearsal plays the whole demo unattended in about 5 minutes,
   so every step has finished runs and asset events to show, then ends with `./demo.sh reset`,
   so you start from `demo1`. Check its output against this one (from the November 3 run at 13:00,
   which started from `demo3`, so its first reset waited about 2 minutes for the old watchers):

   ```
   == demo1: a paid order starts ship_orders, a refund starts nothing
     ship_orders: success
     refund: no asset event, no run
   == demo2-before: a second watcher on the same topic drops events
     Airflow stored 1 of 6 events (0 paid, 1 refunds)
   == demo2: one shared consumer, nothing dropped
     Airflow stored 6 of 6 events (3 paid, 3 refunds)
     bad message in the dead-letter topic: 1
   == demo3-before: the cursor in memory, lost on restart
     ...
     after the restart, rows 1 to 3 were reported again
   == demo3: the cursor in the asset state store, kept across restarts
     ...
     no run after the restart, cursor: {'last_seen_id': 4}
   ...
   asset events no run took, dropped: 4
   ready: demo1, one watcher, one consumer
   rehearsal done in 7m29s
   no failed task attempts
   ```

   `demo2-before` must store fewer than 6. It depends on which watcher holds the partition, so if it
   ever stores 6 the step says so. Live, send `./demo.sh burst` again: every burst in testing lost
   at least one event.
3. Open the Airflow UI on the Assets page, a terminal in this folder for `./demo.sh`, a second one
   on `./demo.sh trigger-logs`, and the editor on `versions/` and `include/`.
4. Record a backup video of the full demo.

## Follow the demo

### First demo: ship an order the moment it's paid

```bash
./demo.sh order paid       # ship_orders starts within a second or two
./demo.sh order refunded   # nothing runs: is_paid_order returned None
./demo.sh received         # 0 rows: it counts from the last order, the refund, which stored nothing
```

The `ship` task log says `Shipping order <id> to <customer> (<amount> USD)`. The `paid_orders`
asset page shows the watcher and each event, with the order in `extra["payload"]`.

### Second demo: a second watcher on the same topic

```bash
./demo.sh use demo2-before   # before the fix
./demo.sh consumers        # wait until it lists two members: one has the partition, one has none
./demo.sh burst            # three paid orders, then three refunds
./demo.sh received         # after about 40 seconds: 2 of 6 in most test runs, 1 once
```

If `received` says Kafka isn't done, the watchers are still reading the burst: run it again a few
seconds later. Which two survive depends on which watcher holds the partition: it commits every
message it reads, including the ones its filter skips. Test runs stored 2 refunds and no paid
order four times, 2 paid orders and no refund once, and 1 refund once.

```bash
./demo.sh use demo2          # after the fix
./demo.sh consumers        # wait until it lists one member, then about 10 seconds more
./demo.sh burst
./demo.sh received         # 6 of 6
./demo.sh bad-message      # a message that isn't JSON
./demo.sh dlq              # it's in orders_dlq, and the offset moved past it
```

The extra 10 seconds cover the shared-stream grace period (5 seconds in `.env`), so both watchers
have subscribed before the first poll.

### Third demo: a watcher with a memory

```bash
./demo.sh use demo3-before # before the fix. First run: restocks the two rows already there
./demo.sh return           # one run, one row
./demo.sh restart-triggerer # first poll after the restart: Checking returns after id 0
./demo.sh received         # "rows 1 to 3": every return reported again after the restart
```

`restart-triggerer` waits for the watcher's first poll after the restart and prints it. The trigger
log has that line only once: every later poll says `after id 3`, and a `trigger-logs` started
before the restart stops with the container.

```bash
./demo.sh use demo3        # after the fix. First run: rows 1 to 3, the store starts empty
./demo.sh return           # one run, one row
./demo.sh cursor           # last_seen_id = 4  (written by the watcher at ...)
./demo.sh restart-triggerer # first poll after the restart: Checking returns after id 4
```

No run starts after the restart. The `restock` task prints `Watcher cursor: <n>`: it reads the same
store through `context["asset_state_store"]`.

## Other commands

| Command | What it does |
|---|---|
| `./demo.sh reset` | Back to `demo1`: two rows in `returns`, empty asset state store, empty dead-letter topic, and no asset event left waiting for a run (see "A run takes the events up to the first one" below). Waits until only the first demo's watcher runs (up to a few minutes after the other demos, see below). Keeps the run history |
| `./demo.sh returns` | Show the `returns` table |
| `./demo.sh trigger-logs` | Follow the triggerer logs, where the watchers run |
| `./demo.sh wipe-runs` | Switch to `demo1`, then delete the run history of the demo Dags. A Dag with a running task is kept, and the command says so: wait, then run it again. Run `./demo.sh rehearse` afterwards |
| `astro dev kill` | Remove all containers and volumes (Kafka and shop data included) |

## Things learned while building (good Q&A material)

- **`MessageQueueTrigger` for Kafka commits before Airflow stores the event.** Its
  `AwaitMessageTrigger` commits the offset of every message it reads, matching or not, and yields
  the event after the commit. A crash in between loses that message. It also exits after each
  event, so the triggerer builds a new trigger (and a new consumer) every time
  (`trigger completed`, then `trigger ID <n> starting`).
- **Two watchers with one connection compete.** Each `MessageQueueTrigger` opens its own consumer,
  and the same connection means the same `group.id`. Kafka hands a partition to one consumer of a
  group, so one watcher reads every message and commits the ones its filter rejects. Each time that
  watcher's trigger restarts after an event, the partition moves to the other watcher for a few
  seconds, and it drops a message of the first watcher's type the same way. That's why the burst
  stores 2 of 6 or fewer, not 3. Nothing logs an error. Separate groups avoid it at the cost of one
  consumer per watcher.
- **The shared-stream trigger needs `enable.auto.commit: "false"`.** `KafkaSharedStreamTrigger`
  commits offsets itself, once every watcher is done with the message and its events are stored,
  and it refuses to start otherwise.
- **Set a cohort grace period.** `[triggerer] shared_stream_cohort_grace_period` defaults to 0. A
  new shared stream waits that long before its first poll, each time one starts, so every watcher
  on the topic can subscribe. A watcher that subscribes later misses the messages already
  committed. 5 seconds here.
- **A run takes the events up to the first one that queued it.** In 3.3.2 the scheduler gives an
  asset-triggered run the events stored up to the `created_at` of the Dag's queue row. The first
  event sets it, and later events don't move it. Events stored after that and before the run is
  created aren't in the run, and they wait until another event queues the next run, which then
  carries them all. In testing, a burst of three paid orders gave a run with one order, and the
  next paid order's run had three. Upstream:
  [#54659](https://github.com/apache/airflow/issues/54659) (fixed in 3.4.0 by
  [#70972](https://github.com/apache/airflow/pull/70972)) and
  [#56050](https://github.com/apache/airflow/issues/56050) (open). That's why the second demo
  checks stored asset events (`received`), not task logs, and why `reset` drops the events no run
  took: otherwise the first live order's run would also ship orders left from the rehearsal.
- **One run can carry several events.** So tasks loop over `triggering_asset_events[asset]`.
- **A removed watcher lingers for about 2 minutes.** When no Dag schedules an asset anymore, its
  watcher keeps running until Airflow's cleanup removes it. `reset` waits for that. Going forward
  through the demos never removes an asset, so it doesn't show during the demo.
- **Masked passwords.** Airflow hides connection passwords in every log line, so the shop password
  is not a word that appears in logs (`shop` would turn every "shop" into `***`).
- **The local REST API needs no token.** The Astro CLI's local Airflow lets every request through
  as admin, so `curl http://localhost:8082/api/v2/...` works as is.
- **When the cursor is saved.** `StatefulReturnsTrigger` yields the event, then calls `aset`. The
  yield returns as soon as the triggerer has queued the event, and the event reaches the database
  on the triggerer's next sync, about a second later. A crash before `aset` reports the rows
  again, and a crash in that second skips them. Keep the task safe to run twice.
- **One asset per stateful trigger.** Airflow keeps one trigger row for identical triggers, even on
  two assets. The asset state store then can't tell which asset's cursor to use and raises a
  `ValueError`.
- **Several triggerers (not tested here).** A stream is shared inside one triggerer process.
  Watchers on the same topic that land on different triggerers each open a consumer in the same
  group, which brings the partition split back. Seen in the source code only: test it before relying
  on it in a high-availability setup.
