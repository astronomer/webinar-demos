from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

_HTML = (Path(__file__).parent / "static" / "schema.html").read_text()

app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def schema_viewer():
    return HTMLResponse(content=_HTML)


class SchemaViewerPlugin(AirflowPlugin):
    name = "schema_viewer"

    fastapi_apps = [{
        "app": app,
        "url_prefix": "/schema-viewer",
        "name": "AstroTrips Schema",
    }]
    external_views = [{
        "name": "Schema",
        "href": "schema-viewer/",
        "destination": "nav",
        "url_route": "schema-viewer",
    }]
