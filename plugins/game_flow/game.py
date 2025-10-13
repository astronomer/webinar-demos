from pathlib import Path

import requests
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import FileResponse
from starlette.responses import StreamingResponse
from starlette.staticfiles import StaticFiles

PLUGIN_DIR = Path(__file__).parent
app = FastAPI(title="GameFlow", version="0.1.0")

# Mount static files
app.mount("/static", StaticFiles(directory=str(PLUGIN_DIR / "static")), name="static")

def _stream_proxy(url: str, filename: str) -> StreamingResponse:
    r = requests.get(url, stream=True)
    r.raise_for_status()
    return StreamingResponse(
        r.iter_content(chunk_size=8192),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Access-Control-Allow-Origin": "*",
        },
    )

@app.get("/doom.zip")
async def doom_zip():
    return _stream_proxy("https://image.dosgamesarchive.com/games/doom-box.zip", "doom.zip")

@app.get("/giana.zip")
async def giana_zip():
    return _stream_proxy("https://image.dosgamesarchive.com/games/giana-gus.zip", "giana.zip")

@app.get("/war2.zip")
async def giana_zip():
    return _stream_proxy("https://image.dosgamesarchive.com/games/war2demo-box.zip", "war2.zip")

@app.get("/")
async def root():
    return {"message": "GameFlow Plugin Active"}

# class GameFlow(AirflowPlugin):
#     name = "game_flow_plugin"
#     fastapi_apps = [{"app": app, "url_prefix": "/gameflow", "name": "GameFlow"}]
#     react_apps = [
#         { "name": "Doom", "bundle_url": "/gameflow/doom.js", "destination": "dag", "url_route": "dag-doom", },
#         { "name": "Doom", "bundle_url": "/gameflow/doom.js", "destination": "nav", "url_route": "doom", },
#         { "name": "Giana Sisters", "bundle_url": "/gameflow/giana.js", "destination": "dag", "url_route": "dag-giana", },
#         { "name": "Giana Sisters", "bundle_url": "/gameflow/giana.js", "destination": "nav", "url_route": "giana", },
#         { "name": "Warcraft II", "bundle_url": "/gameflow/war2.js", "destination": "dag", "url_route": "dag-war2", },
#         { "name": "Warcraft II", "bundle_url": "/gameflow/war2.js", "destination": "nav", "url_route": "war2", },
#     ]
