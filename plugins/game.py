from pathlib import Path

import requests
from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import FileResponse
from starlette.responses import StreamingResponse

PLUGIN_DIR = Path(__file__).parent
app = FastAPI(title="GameFlow", version="0.1.0")

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

def _serve_static(name: str, media_type: str) -> FileResponse:
    return FileResponse(
        path=PLUGIN_DIR / name,
        media_type=media_type,
        filename=name,
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

@app.get("/game.css")
async def game_css():
    return _serve_static("game.css", "text/css")

@app.get("/game.js")
async def game_js():
    return _serve_static("game.js", "application/javascript")

@app.get("/doom.js")
async def doom_js():
    return _serve_static("doom.js", "application/javascript")

@app.get("/giana.js")
async def giana_js():
    return _serve_static("giana.js", "application/javascript")

@app.get("/war2.js")
async def war2_js():
    return _serve_static("war2.js", "application/javascript")

@app.get("/placeholder.jpg")
async def placeholder_image():
    return _serve_static("placeholder.jpg", "image/jpeg")

@app.get("/")
async def root():
    return {"message": "GameFlow Plugin Active"}

class GameFlow(AirflowPlugin):
    name = "GameFlow"
    fastapi_apps = [{"app": app, "url_prefix": "/gameflow", "name": "GameFlow"}]
    react_apps = [
        { "name": "Doom", "bundle_url": "/gameflow/doom.js", "destination": "dag", "url_route": "dag-doom", },
        { "name": "Doom", "bundle_url": "/gameflow/doom.js", "destination": "nav", "url_route": "doom", },
        { "name": "Giana Sisters", "bundle_url": "/gameflow/giana.js", "destination": "dag", "url_route": "dag-giana", },
        { "name": "Giana Sisters", "bundle_url": "/gameflow/giana.js", "destination": "nav", "url_route": "giana", },
        { "name": "Warcraft II", "bundle_url": "/gameflow/war2.js", "destination": "dag", "url_route": "dag-war2", },
        { "name": "Warcraft II", "bundle_url": "/gameflow/war2.js", "destination": "nav", "url_route": "war2", },
    ]
