from flask import Flask, request, send_file, jsonify
from werkzeug.exceptions import BadRequest
import os
from datetime import datetime
import asyncio
import json
from orchestrator import TaskOrchestrator
from operations import OPERATIONS

app = Flask(__name__)

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

if __name__ == "__main__":
    os.makedirs("./results", exist_ok=True)
    app.run(host="0.0.0.0", port=5000, debug=True)
