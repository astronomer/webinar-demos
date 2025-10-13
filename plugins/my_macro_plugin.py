from airflow.plugins_manager import AirflowPlugin


def format_confidence(confidence: float) -> str:
    return f"{confidence * 100:.2f}%"


class MyMacroPlugin(AirflowPlugin):
    name = "my_macro_plugin"
    macros = [format_confidence]
