from datetime import timedelta

from airflow.sdk import dag, task


@dag(tags=["task state store", "spark"])
def task_state_store_spark_example():

    @task
    def extract() -> list[dict]:
        return [
            {"id": 1, "name": "John Doe", "age": 21},
            {"id": 2, "name": "Jane Doe", "age": 22},
            {"id": 3, "name": "Joe Bloggs", "age": 23},
        ]

    @task.pyspark(conn_id="spark_default", retries=2, retry_delay=timedelta(seconds=30))
    def transform(records, spark, **context):

        spark_job_id = context["task_state_store"].get("spark_job_id")

        if spark_job_id:
            print("Spark job already ran on a previous attempt")
            print(f"Continuing job: {spark_job_id}")
        else:
            spark_job_id = spark.conf.get("spark.app.id")
            print(f"Spark job (application) id: {spark_job_id}")
            context["task_state_store"].set("spark_job_id", spark_job_id)

        df = spark.createDataFrame(
            [(r["id"], r["name"], r["age"]) for r in records],
            ["id", "name", "age"],
        )
        transformed = df.filter(df.age > 21)
        transformed.show()

        if context["ti"].try_number == 1:
            raise RuntimeError("Simulated failure on first attempt; retrying.")

        return transformed.toPandas().to_dict(orient="records")

    @task
    def load(rows: list[dict]):
        print(f"Loaded {len(rows)} rows:")
        for row in rows:
            print(row)

    load(transform(extract()))


task_state_store_spark_example()