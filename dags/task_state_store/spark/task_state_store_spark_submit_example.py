from datetime import timedelta

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator



@dag(tags=["task state store", "spark"])
def task_state_store_spark_submit_example():

    SparkSubmitOperator(
        task_id="submit_resumable_spark_job",
        conn_id="spark_standalone",
        application="/opt/spark/examples/jars/spark-examples_2.13-4.0.0.jar",
        java_class="org.apache.spark.examples.SparkPi",
        application_args=["1000"],
        deploy_mode="cluster",
        name="task-state-store-pi",
        status_poll_interval=5,
        reconnect_on_retry=True,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )


task_state_store_spark_submit_example()
