async def custom_async_callback(**kwargs):
    """Handle deadline violation with custom logic."""
    print(f"Deadline exceeded for Dag {kwargs.get("dag_id")}!")
    print(f"Alert type: {kwargs.get("alert_type")}")


def custom_sync_callback(**kwargs):
    """Handle deadline violation with custom logic."""
    print(f"Deadline exceeded for Dag {kwargs.get("dag_id")}!")
    print(f"Alert type: {kwargs.get("alert_type")}")