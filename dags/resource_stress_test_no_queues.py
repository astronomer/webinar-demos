"""
Resource stress test - four tasks stressing different K8s pod resources.

control:      baseline, succeeds.
high_cpu:     pure-Python compute loop, low RAM, no I/O. Expected to succeed but slowly.
high_ram:     allocates ~40GB list, expected OOMKilled / exit 137.
high_storage: writes ~50GB to /tmp, expected ENOSPC (errno 28) or pod eviction for ephemeral-storage limit.
"""
from airflow.decorators import dag, task
from pendulum import datetime
import logging
import os

log = logging.getLogger("airflow.task")


@dag(tags=["resource-stress-test"])
def resource_stress_test_no_queues():

    @task
    def control() -> int:
        return sum(range(1000))

    @task
    def high_cpu() -> None:
        result = 0
        for i in range(10**8):
            result += i * i
        log.info("high_cpu finished, result modulo 1M = %s", result % 1_000_000)

    @task
    def high_ram() -> int:
        log.info("allocating ~40GB list of int references")
        big = [0] * (5 * 10**9)
        return len(big)

    @task
    def high_storage() -> int:
        path = "/tmp/big_file.bin"
        chunk = b"\0" * (1024 * 1024)
        target_mb = 50 * 1024
        try:
            with open(path, "wb") as f:
                for i in range(target_mb):
                    f.write(chunk)
                    if i % 5120 == 0:
                        log.info("wrote %s GB", i // 1024)
            return os.path.getsize(path)
        finally:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

    control()
    high_cpu()
    high_ram()
    high_storage()


resource_stress_test_no_queues()
