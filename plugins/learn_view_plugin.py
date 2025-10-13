from airflow.plugins_manager import AirflowPlugin


class LearnViewPlugin(AirflowPlugin):
    name = "astro_learn_external_view"

    external_views = [
        {
            "name": "📖 Learn Airflow 3",
            "href": "https://www.astronomer.io/docs/learn",
            "destination": "dag",
            "url_route": "learn"
        }
    ]
