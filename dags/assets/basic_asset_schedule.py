from airflow.sdk import dag, Asset, task


@dag(tags=["webinar"])
def my_etl_dag():

    @task
    def extract():
        return {"a": 1, "b": 2}

    @task
    def transform(data):
        return sum(data.values())

    @task(outlets=[Asset("data_ready")])
    def load(data):
        return data

    _extract = extract()
    _transform = transform(_extract)
    load(_transform)


my_etl_dag()


@dag(schedule=[Asset("data_ready")], tags=["webinar"])
def my_ml_dag():

    @task
    def placeholder():
        pass

    placeholder()


my_ml_dag()
