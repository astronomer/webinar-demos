"""
Synthetic resource profiles for the Astro per-task resource metrics view.

Five tasks hold a CPU duty cycle and a memory allocation for hold_seconds, then
exit; cpu_burst_inside_each_run changes both partway through. set_run_index
maintains the run counter and allocates nothing. Two of the five take their
profile from that counter, so the per-task charts change shape across runs with
no edit to this file. MiB means 2^20 bytes. CPU figures are fractions of one
core.

Profiles. Allocated is what the task requests. Reported is that allocation plus
the worker process footprint, measured at 353 MiB on 2026-08-25 as the median
for start_run in the predecessor DAG resource_metrics_story, which allocated
nothing. set_run_index should reproduce that figure but has not been measured.
The footprint is not a constant; recalibrate on another deployment.

| Task                      | CPU                           | Allocated MiB               | Reported MiB                |
|---------------------------|-------------------------------|-----------------------------|-----------------------------|
| steady_cpu_and_memory     | 0.30 every run                | 300                         | ~650                        |
| memory_grows_every_run    | 0.20 every run                | 120 + 100 per run, cap 1020 | ~470 rising to ~1370        |
| cpu_spike_every_third_run | 0.12; 0.90 on runs 3, 6, 9    | 200                         | ~550                        |
| cpu_burst_inside_each_run | 0.10; 0.92 for 35% of the run | 250; 550 during the burst   | ~600; ~900 during the burst |
| almost_no_resources       | 0.05 every run                | 80                          | ~430                        |

Notes:

  Run in parallel, the five allocations sum on one worker. The predecessor DAG
  resource_metrics_story did that with an uncapped ramp, and on 2026-08-25 its
  ramp task enrich_customer_profiles reported far below its request at every
  index while the other four matched prediction, which is what a kill during
  allocation looks like when the sampler never observes the peak. The five
  parallel allocations came to roughly 3.4 GiB at that index, and the queue
  those runs landed on had 4 GiB per worker on 2026-08-27. Inferred from the
  charts and that queue configuration; not confirmed against a task log.

  _allocate writes one byte per 4 KiB page after allocating. That loop is
  redundant: CPython zero-fills bytearray(n), so the buffer is already resident
  on return. Measured on CPython 3.9.6 under macOS, where RSS rose by the full
  600 MiB before the loop ran. It is kept because residency has not been
  measured on the Runtime image, and its cost is negligible against
  hold_seconds.

  _burn is single threaded and paced against a wall clock. It targets a fraction
  of one core and cannot exceed it. Under contention it undershoots the target
  rather than extending the task.

  RAMP_CAP_MIB bounds worst case residency at the cap plus the worker footprint,
  about 1.4 GiB at mem_scale 1.0. Run 11 onward repeats run 10's value. Check
  the target queue's memory per worker and its task concurrency before raising
  RAMP_STEP_MIB; the headroom is shared with any co-tenant task. The queue used
  here had 4 GiB and allowed 10 concurrent tasks on 2026-08-27.

Limitations:

  The counter is a mutable Variable incremented inside a task, so this DAG is
  not idempotent. Clearing or retrying set_run_index increments it again, and
  concurrent runs would race for it. retries=0 and max_active_runs=1 contain
  that; nothing else does.

  hold_seconds has a lower bound set by the metrics collector's sampling
  interval, which is not documented. An 18 s burst inside a 90 s task was
  resolved correctly on 2026-08-25, so 18 s is known to be sufficient and
  shorter windows are untested. The 60 s default puts the burst at 21 s, above
  that mark; 45 s would put it at 15.75 s, below it.

Params
  hold_seconds  Seconds each task holds its profile. Default 60. Read the
                sampling note before lowering it.
  mem_scale     Multiplier on every allocation. Default 1.0. Reported shifts by
                the same amount, not the same factor, since the worker footprint
                is unaffected.

"""

from __future__ import annotations

import logging
import time

from airflow.sdk import Variable, dag, get_current_context, task

log = logging.getLogger("airflow.task")

RUN_INDEX_VARIABLE = "task_resource_demo_run_index"

RAMP_START_MIB = 120
RAMP_STEP_MIB = 100
RAMP_CAP_MIB = 1020


def _rss_mib() -> int:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    return -1


def _burn(target_cpu: float, seconds: float) -> None:
    """Hold a wall-clock CPU duty cycle, so the shape does not depend on core speed."""
    target_cpu = max(0.0, min(target_cpu, 0.95))
    slice_s = 0.25
    deadline = time.monotonic() + seconds
    x = 1.000001
    while time.monotonic() < deadline:
        busy_until = time.monotonic() + slice_s * target_cpu
        while time.monotonic() < busy_until:
            for _ in range(2000):
                x = x * 1.0000001 + 0.5
        idle = slice_s * (1.0 - target_cpu)
        if idle > 0:
            time.sleep(idle)


def _allocate(mib: int) -> bytearray:
    """Allocate and touch every page so the memory is actually resident."""
    buf = bytearray(mib * 1024 * 1024)
    for offset in range(0, len(buf), 4096):
        buf[offset] = 1
    return buf


@dag(
    schedule=None,
    max_active_runs=1,
    default_args={"retries": 0},
    params={"hold_seconds": 60, "mem_scale": 1.0},
    tags=["resource-metrics", "demo"],
)
def resource_metrics_demo():

    @task
    def set_run_index() -> dict:
        try:
            previous = int(Variable.get(RUN_INDEX_VARIABLE))
        except Exception:
            previous = 0
        index = previous + 1
        Variable.set(RUN_INDEX_VARIABLE, str(index))

        params = get_current_context()["params"]
        plan = {
            "index": index,
            "hold_seconds": float(params["hold_seconds"]),
            "mem_scale": float(params["mem_scale"]),
        }
        log.info("resource metrics demo run %s, plan=%s", index, plan)
        return plan

    @task
    def steady_cpu_and_memory(plan: dict) -> None:
        mib = int(300 * plan["mem_scale"])
        buf = _allocate(mib)
        log.info("run %s: 0.30 cores, allocated %s MiB, rss %s MiB", plan["index"], mib, _rss_mib())
        _burn(0.30, plan["hold_seconds"])
        del buf

    @task
    def memory_grows_every_run(plan: dict) -> None:
        raw = RAMP_START_MIB + RAMP_STEP_MIB * (plan["index"] - 1)
        mib = int(min(raw, RAMP_CAP_MIB) * plan["mem_scale"])
        buf = _allocate(mib)
        log.info(
            "run %s: 0.20 cores, allocated %s MiB (uncapped %s), rss %s MiB",
            plan["index"],
            mib,
            raw,
            _rss_mib(),
        )
        _burn(0.20, plan["hold_seconds"])
        del buf

    @task
    def cpu_spike_every_third_run(plan: dict) -> None:
        spike = plan["index"] % 3 == 0
        cpu = 0.90 if spike else 0.12
        mib = int(200 * plan["mem_scale"])
        buf = _allocate(mib)
        log.info(
            "run %s: %s cores (%s), allocated %s MiB, rss %s MiB",
            plan["index"],
            cpu,
            "spike run" if spike else "normal run",
            mib,
            _rss_mib(),
        )
        _burn(cpu, plan["hold_seconds"])
        del buf

    @task
    def cpu_burst_inside_each_run(plan: dict) -> None:
        hold = plan["hold_seconds"]
        base_mib = int(250 * plan["mem_scale"])
        burst_mib = int(300 * plan["mem_scale"])
        buf = _allocate(base_mib)
        log.info(
            "run %s: 0.10 cores at %s MiB, bursting to 0.92 cores at %s MiB, rss %s MiB",
            plan["index"],
            base_mib,
            base_mib + burst_mib,
            _rss_mib(),
        )
        _burn(0.10, hold * 0.325)
        burst = _allocate(burst_mib)
        _burn(0.92, hold * 0.35)
        del burst
        _burn(0.10, hold * 0.325)
        del buf

    @task
    def almost_no_resources(plan: dict) -> None:
        mib = int(80 * plan["mem_scale"])
        buf = _allocate(mib)
        log.info("run %s: 0.05 cores, allocated %s MiB, rss %s MiB", plan["index"], mib, _rss_mib())
        _burn(0.05, plan["hold_seconds"])
        del buf

    plan = set_run_index()
    steady = steady_cpu_and_memory(plan)
    ramp = memory_grows_every_run(plan)
    spike = cpu_spike_every_third_run(plan)
    burst = cpu_burst_inside_each_run(plan)
    idle = almost_no_resources(plan)

    steady >> ramp >> spike >> burst >> idle


resource_metrics_demo()
