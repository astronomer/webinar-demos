from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

PLUGIN_DIR = Path(__file__).parent
app = FastAPI(title="DAG Toggle Plugin", version="0.1.0")

# Mount static files
app.mount("/static", StaticFiles(directory=str(PLUGIN_DIR / "static")))

@app.get("/")
async def root():
    """Redirect to the toggle interface"""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/dag-toggle/interface")

@app.get("/status")
async def get_dag_status():
    """Get the overall status of all DAGs (are they all paused or not?)"""
    try:
        from airflow.models import DagBag, DagModel
        from airflow.utils.db import provide_session

        @provide_session
        def check_all_paused(session=None):
            dagbag = DagBag()
            total_dags = len(dagbag.dags)
            if total_dags == 0:
                return {"all_paused": False, "total_dags": 0, "paused_dags": 0}

            paused_count = 0
            for dag_id in dagbag.dags.keys():
                dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                if dag_model and dag_model.is_paused:
                    paused_count += 1

            return {
                "all_paused": paused_count == total_dags,
                "total_dags": total_dags,
                "paused_dags": paused_count
            }

        return check_all_paused()
    except Exception as e:
        return {"error": f"Could not check DAG status: {str(e)}", "all_paused": False}

@app.post("/pause-all")
async def pause_all_dags():
    """Pause all DAGs in the environment"""
    try:
        from airflow.models import DagBag, DagModel
        from airflow.utils.db import provide_session

        @provide_session
        def pause_dags(session=None):
            dagbag = DagBag()
            paused_count = 0

            for dag_id in dagbag.dags.keys():
                dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                if dag_model and not dag_model.is_paused:
                    dag_model.is_paused = True
                    paused_count += 1

            session.commit()
            return paused_count

        paused_count = pause_dags()
        return {
            "message": f"Successfully paused {paused_count} DAGs",
            "paused_count": paused_count,
            "status": "success"
        }
    except Exception as e:
        return {"error": f"Failed to pause DAGs: {str(e)}", "status": "error"}

@app.post("/unpause-all")
async def unpause_all_dags():
    """Unpause all DAGs in the environment"""
    try:
        from airflow.models import DagBag, DagModel
        from airflow.utils.db import provide_session

        @provide_session
        def unpause_dags(session=None):
            dagbag = DagBag()
            unpaused_count = 0

            for dag_id in dagbag.dags.keys():
                dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                if dag_model and dag_model.is_paused:
                    dag_model.is_paused = False
                    unpaused_count += 1

            session.commit()
            return unpaused_count

        unpaused_count = unpause_dags()
        return {
            "message": f"Successfully unpaused {unpaused_count} DAGs",
            "unpaused_count": unpaused_count,
            "status": "success"
        }
    except Exception as e:
        return {"error": f"Failed to unpause DAGs: {str(e)}", "status": "error"}

@app.post("/toggle")
async def toggle_all_dags():
    """Toggle all DAGs - if all are paused, unpause them; if any are running, pause all"""
    try:
        from airflow.models import DagBag, DagModel
        from airflow.utils.db import provide_session

        @provide_session
        def perform_toggle(session=None):
            dagbag = DagBag()
            total_dags = len(dagbag.dags)
            if total_dags == 0:
                return {"message": "No DAGs found", "action": "none", "count": 0}

            # Check current state
            paused_count = 0
            for dag_id in dagbag.dags.keys():
                dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                if dag_model and dag_model.is_paused:
                    paused_count += 1

            all_paused = paused_count == total_dags

            # Toggle: if all paused, unpause all; otherwise pause all
            action_count = 0
            if all_paused:
                # Unpause all
                for dag_id in dagbag.dags.keys():
                    dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                    if dag_model and dag_model.is_paused:
                        dag_model.is_paused = False
                        action_count += 1
                action = "unpaused"
            else:
                # Pause all
                for dag_id in dagbag.dags.keys():
                    dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                    if dag_model and not dag_model.is_paused:
                        dag_model.is_paused = True
                        action_count += 1
                action = "paused"

            session.commit()
            return {
                "message": f"Successfully {action} {action_count} DAGs",
                "action": action,
                "count": action_count,
                "status": "success"
            }

        return perform_toggle()
    except Exception as e:
        return {"error": f"Failed to toggle DAGs: {str(e)}", "status": "error"}

@app.get("/interface")
async def toggle_page():
    """Minimalist DAG Toggle Interface - loads from template file"""
    try:
        template_path = PLUGIN_DIR / "templates" / "interface.html"
        with open(template_path, "r") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except Exception as e:
        error_html = f"""
        <html><body>
        <h1>⚠️ Template Error</h1>
        <p>Could not load template: {str(e)}</p>
        <p><a href="javascript:location.reload()">🔄 Reload Page</a></p>
        </body></html>
        """
        return HTMLResponse(content=error_html)

@app.get("/widget.js")
async def dag_toggle_widget():
    return FileResponse(
        path=PLUGIN_DIR / "static" / "widget.js",
        media_type="text/javascript",
        filename="widget.js",
    )

# class DAGTogglePlugin(AirflowPlugin):
#     name = "dag_toggle_plugin"
#
#     # FastAPI app for the toggle functionality
#     fastapi_apps = [{
#         "app": app,
#         "url_prefix": "/dag-toggle",
#         "name": "DAG Toggle Plugin"
#     }]
#
#     # External view - simple toggle interface accessible from navigation
#     external_views = [{
#         "name": "All DAGs Toggle",
#         "href": "/dag-toggle/interface",
#         "destination": "nav",
#         "category": "admin",
#         "url_route": "dag_toggle"
#     }]
#
#     # React app - widget embedded directly on dashboard
#     react_apps = [{
#         "name": "DAG Toggle Widget",
#         "bundle_url": "/dag-toggle/widget.js",
#         "destination": "dashboard"
#     }]
