from quart import Quart, request, render_template, send_from_directory, abort, url_for, jsonify, send_file
from werkzeug.exceptions import BadRequest
import os
import aiosqlite
from datetime import datetime
import asyncio
import sqlite3
import json
from orchestrator import TaskOrchestrator
from operations import OPERATIONS
import pydot
import aiofiles
from asgiref.wsgi import WsgiToAsgi
from otel_config import configure_opentelemetry, get_tracer, get_meter
import logging

logger = logging.getLogger("taskflow")
logger.setLevel(logging.INFO)

if not logger.hasHandlers():
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


configure_opentelemetry(service_name="taskflow")

tracer = get_tracer("taskflow")
meter = get_meter("taskflow")
api_cli_req_counter = meter.create_counter("api_cli_req_counter")
api_web_req_counter = meter.create_counter("api_web_req_counter")

app = Quart(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DAGS_DIR = os.path.join(BASE_DIR, 'dags')
DB_PATH = os.path.join(BASE_DIR, 'orchestrator.db')


# "/api/cli" logic
@app.route("/api/cli", methods=["POST"])
async def run_cli():
    api_cli_req_counter.add(1)
    config = await request.get_json()
    if not config:
        raise BadRequest("JSON body is required")

    try:
        orchestrator = TaskOrchestrator(
            dag_config=config,
            operations=OPERATIONS,
        )
        dag_id = orchestrator.dag_id
        await orchestrator.execute_dag(recovery_mode=False)
        with tracer.start_as_current_span(f"dag.run") as span:
            span.set_attribute("dag.id", dag_id)
            logger.info(f"DAG {dag_id} по ручке /api/cli запущен")
        return await send_from_directory(DAGS_DIR, f"{dag_id}.zip", as_attachment=True)
    except Exception as e:

        return {"error": str(e)}, 500


@app.route("/")
def index():
    return {"status": "ok"}


# "/api/web" logic



def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # чтобы row["status"] работало
    return conn


# Асинхронный хелпер для загрузки конфига DAG
async def load_dag_config(dag_id):
    dag_path = os.path.join(DAGS_DIR, dag_id)
    config_path = os.path.join(dag_path, "config.json")

    if not os.path.exists(config_path):
        return None
    async with aiofiles.open(config_path, 'r') as f:
        content = await f.read()
        output = json.loads(content)

        return json.loads(content)


# Асинхронный хелпер для загрузки results.json, если существует
async def load_dag_results(dag_id):
    results_path = os.path.join(DAGS_DIR, dag_id, 'results.json')
    if os.path.exists(results_path):
        async with aiofiles.open(results_path, 'r') as f:
            content = await f.read()
            return json.loads(content)
    return {}


async def is_dag_complete(dag_id):
    zip_path = os.path.join(DAGS_DIR, f"{dag_id}.zip")
    if os.path.exists(zip_path):
        return True
    return False


# Хелпер для генерации SVG-графа с pydot
async def generate_dag_graph(dag_id, config):
    graph = pydot.Dot(graph_type='digraph', rankdir='LR')
    conn = db_connect()
    cursor = conn.cursor()
    task_nodes = {}
    with conn:

        config = dict(config)
        for task in config["tasks"]:
            cursor.execute(f"SELECT status FROM {dag_id} WHERE task_id = ?", (task["id"],))
            row = cursor.fetchone()
            status = row[0] if row else "pending"
            color = "green" if status == "completed" else "lightblue" if status == "running" else "white" if status == "pending" else "red"
            node = pydot.Node(task["id"], shape='box', style='filled', fillcolor=color,
                              label=task['id'] + f"\n({status})")
            task_nodes[task['id']] = node
            graph.add_node(node)
    for task in config['tasks']:
        for dep in task['dependencies']:
            has_data_dep = False
            for dep_param in task['dependent_params'].values():
                if dep in dep_param:
                    has_data_dep = True
                    break
            style = 'solid' if has_data_dep else 'dashed'
            head_shape = 'circle' if has_data_dep else 'diamond'
            tail_shape = 'circle' if has_data_dep else 'diamond'
            edge = pydot.Edge(task_nodes[dep], task_nodes[task['id']], style=style, arrowhead=head_shape,
                              arrowtail=tail_shape)
            graph.add_edge(edge)
    return graph.create_svg().decode('utf-8')


@app.route('/api/web', methods=['POST'])
async def api_web():
    config = await request.get_json()
    api_web_req_counter.add(1)
    if not config or 'dag_name' not in config:
        return jsonify({'error': 'Invalid config'}), 400

    try:
        orchestrator = TaskOrchestrator(
            dag_config=config,
            operations=OPERATIONS,
        )
        dag_id = orchestrator.dag_id
        asyncio.create_task(orchestrator.execute_dag(recovery_mode=False))

        with tracer.start_as_current_span(f"dag.run") as span:
            span.set_attribute("dag.id", dag_id)
            logger.info(f"DAG {dag_id} по ручке /api/web запущен")

        # Возвращаем ссылку
        ui_link = url_for('dag_ui', dag_id=dag_id, _external=True)
        return jsonify({'link': ui_link})
    except Exception as e:
        return {"error": str(e)}, 500


@app.route('/dag_ui/<dag_id>')
async def dag_ui(dag_id):
    config = await load_dag_config(dag_id)

    if not config:
        abort(404)

    # Генерируем граф SVG
    graph_svg = await generate_dag_graph(dag_id=dag_id, config=config)

    # Получаем список задач с ссылками
    tasks = []
    for task in config['tasks']:
        task_link = url_for('task_details', dag_id=dag_id, task_id=task['id'])
        tasks.append({'id': task['id'], 'link': task_link})

    # Проверяем наличие ZIP для скачивания
    show_zip = await is_dag_complete(dag_id)
    zip_link = url_for('download_zip', dag_id=dag_id) if show_zip else None

    # Рендерим с meta-refresh для опроса
    return await render_template('dag_ui.html', dag_id=dag_id, graph_svg=graph_svg, tasks=tasks, show_zip=show_zip,
                           zip_link=zip_link)


@app.route("/dag_ui/<dag_id>/<task_id>")
async def task_details(dag_id, task_id):
    conn = db_connect()
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            f"SELECT status, result, error, params, retry_count FROM {dag_id} WHERE task_id = ?", (task_id,))
        row = cursor.fetchall()
        db_row = row[0] if row else None

    if db_row:
        status = db_row[0]
        result = json.loads(db_row[1]) if db_row[1] else {}
        error = db_row[2]
        params = json.loads(db_row[3]) if db_row[3] else {}
        retry_count = db_row[4]
    else:
        status = 'pending'
        result = {}
        error = None
        params = {}
        retry_count = 0
    if result:
        output_file = result.get('output_file_path', None)
        download_link = url_for('download_file', dag_id=dag_id,
                                filename=os.path.basename(output_file)) if output_file else None
    download_link = None
    return await render_template(
        'task_details.html',
        dag_id=dag_id,
        task_id=task_id,
        params=params,
        result=result,
        status=status,
        error=error,
        retry_count=retry_count,
        download_link=download_link
    )


@app.route('/download/<dag_id>/<filename>')
async def download_file(dag_id, filename):
    dag_dir = os.path.join(DAGS_DIR, dag_id)
    return await send_from_directory(dag_dir, filename, as_attachment=True)


@app.route('/download_zip/<dag_id>')
async def download_zip(dag_id):
    zip_path = os.path.join(DAGS_DIR, f"{dag_id}.zip")
    if not os.path.exists(zip_path):
        abort(404)
    return await send_from_directory(DAGS_DIR, f"{dag_id}.zip", as_attachment=True)


if __name__ == "__main__":
    os.makedirs("./results", exist_ok=True)
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)