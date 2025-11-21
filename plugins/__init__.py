from airflow.plugins_manager import AirflowPlugin

class TypedDictsPlugin(AirflowPlugin):
    name = "typeddicts_plugin"
    # Import your actual file here
    from plugins import typeddicts
