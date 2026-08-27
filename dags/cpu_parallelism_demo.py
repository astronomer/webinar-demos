"""
Shows why one Airflow task can exceed one core and when it cannot.

Three tasks run the same group-and-sum, differing only in what executes the loop.
report_cpu_budget runs first and logs how many cores the pod may actually use;
if that number is at or below 1, the demo cannot show parallelism and the two
polars tasks will look identical to the Python one.

| Task                   | Executes the loop in         | Expected cores        |
|------------------------|------------------------------|-----------------------|
| python_loop            | CPython bytecode, one thread | ~1.0, hard ceiling    |
| polars_one_thread      | Rust, one thread             | ~1.0, same ceiling    |
| polars_all_threads     | Rust, polars_threads threads | ~polars_threads       |

python_loop and polars_one_thread both sit at one core. The difference between
them is throughput, logged as rows per second, and it comes from Rust doing per
row work that CPython cannot. The difference between polars_one_thread and
polars_all_threads is parallelism, and only that one moves the CPU chart above
1.0.

Notes:

  Every task is pinned to one queue, python_loop included, so the three
  workloads are compared on one machine rather than three. That queue needs at
  least polars_threads vCPU; on a 2 vCPU worker polars_all_threads caps at 2.0
  whatever polars_threads says. On the deployment this was written for, as
  configured on 2026-08-27, cpu-intensive was an A40 with 8 vCPU and default was
  an A10 with 2 vCPU. Check the queue sizes and repoint the queue argument
  before running this anywhere else.

  The three workload tasks are chained. Run in parallel they would compete for
  the same cores and polars_all_threads could not reach its thread count.

  POLARS_MAX_THREADS is read by polars at import, so each task sets it before
  importing polars inside the task function. This works because every Airflow
  task instance runs in its own process; polars is deliberately not imported at
  module level.

  Both loops finish the iteration they are in when the deadline passes, so a
  task overruns hold_seconds by up to one iteration. On 15M rows that is a
  second or two for polars and longer for python_loop.

Limitations:

  Co-tenant tasks on the same worker take cores from polars_all_threads, and
  Kubernetes throttles rather than fails, so contention produces a
  correct-looking chart that simply never rises above what was left over. The
  queue used here allowed 5 concurrent tasks per worker on 2026-08-27. Run this
  on a quiet deployment.

  polars_threads is capped by the machine polars sees at import, not by the
  param. Compare the logged thread pool size against the param before reading
  anything into the chart.

Params
  hold_seconds    Seconds each task loops for. Default 60.
  polars_threads  Thread pool size for polars_all_threads. Default 4.
"""

from __future__ import annotations

import logging
import os
import time

from airflow.sdk import dag, get_current_context, task

log = logging.getLogger("airflow.task")

POLARS_ROWS = 15_000_000
PYTHON_ROWS = 1_000_000
GROUP_MODULUS = 997


def _cpu_quota() -> str:
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().split()
        if quota == "max":
            return "no quota"
        return "%.2f cores" % (int(quota) / int(period))
    except OSError:
        pass
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read())
        return "no quota" if quota < 0 else "%.2f cores" % (quota / period)
    except OSError:
        return "unreadable"


def _build_polars_frame(pl, rows: int):
    return pl.DataFrame(
        {
            "k": (pl.int_range(0, rows, eager=True) % GROUP_MODULUS),
            "v": pl.int_range(0, rows, eager=True),
        }
    )


def _polars_pass(pl, df) -> int:
    """Eager, not lazy: the optimizer would drop a sort the group_by does not need."""
    ranked = df.sort("v", descending=True)
    out = ranked.group_by("k").agg(
        pl.col("v").sum().alias("total"), pl.col("v").mean().alias("avg")
    )
    return out.height


def _run_polars(pl, hold_seconds: float, label: str) -> None:
    try:
        pool = pl.thread_pool_size()
    except AttributeError:
        pool = os.environ.get("POLARS_MAX_THREADS", "unset")

    df = _build_polars_frame(pl, POLARS_ROWS)
    deadline = time.monotonic() + hold_seconds
    started = time.monotonic()
    passes = 0
    while time.monotonic() < deadline:
        _polars_pass(pl, df)
        passes += 1
    elapsed = time.monotonic() - started
    log.info(
        "%s: %s passes over %s rows in %.1f s, %.0f rows/s, thread pool %s",
        label,
        passes,
        POLARS_ROWS,
        elapsed,
        passes * POLARS_ROWS / elapsed,
        pool,
    )


def _python_pass(keys: list[int], values: list[int]) -> int:
    totals: dict[int, int] = {}
    for k, v in zip(keys, values):
        totals[k] = totals.get(k, 0) + v
    return len(totals)


@dag(
    schedule=None,
    max_active_runs=1,
    default_args={"retries": 0},
    params={"hold_seconds": 60, "polars_threads": 4},
    tags=["resource-metrics", "demo"],
)
def cpu_parallelism_demo():

    @task(queue="cpu-intensive")
    def report_cpu_budget() -> dict:
        params = get_current_context()["params"]
        log.info(
            "os.cpu_count()=%s, cgroup quota=%s, requested polars threads=%s",
            os.cpu_count(),
            _cpu_quota(),
            params["polars_threads"],
        )
        return {
            "hold_seconds": float(params["hold_seconds"]),
            "polars_threads": int(params["polars_threads"]),
        }

    @task(queue="cpu-intensive")
    def python_loop(plan: dict) -> None:
        import polars as pl

        df = _build_polars_frame(pl, PYTHON_ROWS)
        keys = df["k"].to_list()
        values = df["v"].to_list()
        del df

        deadline = time.monotonic() + plan["hold_seconds"]
        started = time.monotonic()
        passes = 0
        while time.monotonic() < deadline:
            _python_pass(keys, values)
            passes += 1
        elapsed = time.monotonic() - started
        log.info(
            "python_loop: %s passes over %s rows in %.1f s, %.0f rows/s, 1 thread",
            passes,
            PYTHON_ROWS,
            elapsed,
            passes * PYTHON_ROWS / elapsed,
        )

    @task(queue="cpu-intensive")
    def polars_one_thread(plan: dict) -> None:
        os.environ["POLARS_MAX_THREADS"] = "1"
        import polars as pl

        _run_polars(pl, plan["hold_seconds"], "polars_one_thread")

    @task(queue="cpu-intensive")
    def polars_all_threads(plan: dict) -> None:
        os.environ["POLARS_MAX_THREADS"] = str(plan["polars_threads"])
        import polars as pl

        _run_polars(pl, plan["hold_seconds"], "polars_all_threads")

    plan = report_cpu_budget()
    one = python_loop(plan)
    two = polars_one_thread(plan)
    three = polars_all_threads(plan)

    one >> two >> three


cpu_parallelism_demo()
