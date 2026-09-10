from datetime import timedelta

from airflow.sdk import dag, task

SPARK_MASTER_REST = "http://spark-master:6066"
SPARK_MASTER_RPC = "spark://spark-master:7077"
APP_JAR = "/opt/spark/examples/jars/spark-examples_2.13-4.0.0.jar"
TERMINAL_OK = {"FINISHED"}
TERMINAL_BAD = {"FAILED", "ERROR", "KILLED"}


@dag(tags=["task state store", "spark"])
def task_state_store_spark_rest_example():

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def submit_and_track(**context):
        import time

        import requests

        tss = context["task_state_store"]
        submission_id = tss.get("spark_job_id")

        if not submission_id:
            body = {
                "action": "CreateSubmissionRequest",
                "appResource": APP_JAR,
                "clientSparkVersion": "4.0.0",
                "mainClass": "org.apache.spark.examples.SparkPi",
                "appArgs": ["1000"],
                "environmentVariables": {"SPARK_ENV_LOADED": "1"},
                "sparkProperties": {
                    "spark.master": SPARK_MASTER_RPC,
                    "spark.app.name": "task-state-store-pi-rest",
                    "spark.submit.deployMode": "cluster",
                    "spark.jars": APP_JAR,
                },
            }
            resp = requests.post(f"{SPARK_MASTER_REST}/v1/submissions/create", json=body, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise RuntimeError(f"Spark REST submit failed: {data}")
            submission_id = data["submissionId"]
            tss.set("spark_job_id", submission_id)
            print(f"Submitted Spark driver: {submission_id}")

            if context["ti"].try_number == 1:
                raise RuntimeError(
                    "Simulated failure right after submit. The driver keeps running on the cluster; "
                    "the retry reconnects via the stored submissionId instead of resubmitting."
                )
        else:
            print(f"Reconnecting to running driver: {submission_id}")

        while True:
            status = requests.get(
                f"{SPARK_MASTER_REST}/v1/submissions/status/{submission_id}", timeout=30
            ).json()
            state = status.get("driverState")
            print(f"Driver {submission_id} state: {state}")
            if state in TERMINAL_OK:
                break
            if state in TERMINAL_BAD:
                raise RuntimeError(f"Driver {submission_id} ended in state {state}")
            time.sleep(5)

    submit_and_track()


task_state_store_spark_rest_example()
