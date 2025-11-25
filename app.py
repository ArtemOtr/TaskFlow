from flask import Flask, request, send_file, jsonify, render_template, send_from_directory, abort, url_for
from werkzeug.exceptions import BadRequest
import os
import aiosqlite
from datetime import datetime
import asyncio
import json
from orchestrator import TaskOrchestrator
from operations import OPERATIONS
import pydot
import aiofiles

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAGS_DIR = os.path.join(BASE_DIR, 'dags')
DB_PATH = os.path.join(BASE_DIR, 'orchestrator.db')

# "/api/cli" logic
@app.route("/api/cli", methods=["POST"])
async def run_cli():
    config = request.get_json()
    if not config:
        raise BadRequest("JSON body is required")


    try:
        orchestrator = TaskOrchestrator(
            dag_config=config,
            operations=OPERATIONS,
        )
        result = await orchestrator.execute_dag(recovery_mode=False)
        zip_path = result["zip_path"]

        return send_file(zip_path, as_attachment=True)
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/")
def index():
    return {"status": "ok"}


# "/api/web" logic

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAGS_DIR = os.path.join(BASE_DIR, 'dags')
DB_PATH = os.path.join(BASE_DIR, 'orchestrator.db')

async def db_connect():
    return await aiosqlite.connect(DB_PATH)

async def load_dag_config(dag_id):
    pass

async def load_dag_results(dag_id):
    pass

async def is_dag_complete(dag_id):
    zip_path = os.path.join(DAGS_DIR, f"{dag_id}.zip")
    if os.path.exists(zip_path):
        return True
    return False

# Хелпер для генерации SVG-графа с pydot (синхронно, так как не I/O)
async def generate_dag_graph(dag_id, config):
    pass


if __name__ == "__main__":
    os.makedirs("./results", exist_ok=True)
    app.run(host="0.0.0.0", port=5000, debug=True)
