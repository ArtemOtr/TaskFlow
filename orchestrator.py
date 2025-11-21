import asyncio
import json
from typing import Dict, List, Any, Callable
import aiosqlite
import time
import inspect
import os

class TaskOrchestrator:
    def __init__(self, dag_config, operations,  dag_id, db_path = "orchestrator.db"):
        self.dag_config = dag_config
        self.results = {}
        self.db_path = db_path
        self.max_retries = dag_config["max_retries"]
        self.retry_delay = dag_config["retry_delay"]
        self.operations = operations
        self.ready_tasks = []
        self.dag_id = dag_id

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            # Создаем таблицу
            await db.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.dag_id} (
                    task_id TEXT PRIMARY KEY,
                    status TEXT,
                    result TEXT,
                    error TEXT,
                    params TEXT,
                    retry_count INTEGER,
                    created_at REAL,
                    updated_at REAL
                )
            ''')

            # Инициализируем все задачи из DAG конфига
            for task in self.dag_config["tasks"]:
                task_id = task["id"]
                params = json.dumps(self._get_funcs_param(task_config=task))
                print("str params", params)
                await db.execute(f'''
                    INSERT OR REPLACE INTO {self.dag_id} 
                    (task_id, status, result, error, params, retry_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    task_id,
                    "pending",
                    None,  # result
                    None,  # error
                    params,
                    0,
                    time.time(),
                    time.time()
                ))
            await db.commit()
            print("hui")

    def _get_funcs_param(self, task_config):
        task_id = task_config["id"]
        operation_name = task_config["operation"]
        operation_func = self.operations[operation_name]
        independent_params = task_config["independent_params"]

        print("independent_params", independent_params)



        default_params = self.get_default_parameter_names(operation_func)


        for key, value in default_params.items():
            if not(key in independent_params):
                independent_params[key] = value

        print("ujiahsdf", independent_params)
        return independent_params


    async def cleanup_db(self):
        """Очистка DB"""


        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute(f'''
                    DELETE FROM {self.dag_id}
                ''')
                await db.commit()
            except aiosqlite.OperationalError as e:
                if "no such table" in str(e):
                    print("Таблица не существует, нечего очищать")
                    pass
                else:
                    raise e

    async def _load_task_state(self, task_id: str) -> Dict:
        """Получает состояние операции из БД"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(f"SELECT status, result, retry_count, params FROM {self.dag_id} WHERE task_id = ?", (task_id, )) as cursor:
                row = await cursor.fetchone()
                if row:
                    params_str = row[3]
                    params_dict = json.loads(params_str)
                    return {
                        "status": row[0],
                        "result": row[1],
                        "retry_count": row[2],
                        "params": params_dict
                    }
        return {}
    async def execute_dag(self, recovery_mode = False):
        """Запуск DAG"""


        print(f"Запуск {self.dag_id}...")

        if not recovery_mode:
            print(f" Новый запуск DAG: {self.dag_id}...")
            await self.cleanup_db()
            await self.init_db()


        tasks = self.dag_config["tasks"]

        self.ready_tasks = await self._find_ready_tasks(tasks)
        await self._execute_tasks(self.ready_tasks)

        print(f"Весь DAG {self.dag_id} выполнен!")

        self.save_all_dag_data_for_user()
        return self.results

    async def _find_ready_tasks(self, tasks):
        ready_tasks = []

        for task in tasks:
            task_id = task["id"]

            deps_ready = all(dep in self.results for dep in task["dependencies"])
            if not deps_ready:
                continue

            state = await self._load_task_state(task_id)

            if state:
                if state["status"] == "completed":
                    continue
                if state["status"] == "running":
                    continue
                if state["status"] == "failed":
                    retry_count = state["retry_count"]
                    if retry_count >= self.max_retries:
                        print(f"{task_id} достигнут лимит повторов. Операция будет пропущена.")
                        continue

            ready_tasks.append(task)
        return ready_tasks


    async def _execute_tasks(self, tasks: List[Dict]):
        """Выполняет асинхронно, поданный список задач"""
        async_tasks = []
        for task_config in tasks:
            async_task = self._execute_single_task(task_config)
            async_tasks.append(async_task)

        await asyncio.gather(*async_tasks)

    def get_default_parameter_names(self, func: Callable[..., Any]) -> dict[str, Any]:
        """Возвращает словарь {имя_параметра: значение_по_умолчанию, если есть}"""
        sig = inspect.signature(func)
        parameters = {}

        for name, param in sig.parameters.items():
            if param.default == inspect.Parameter.empty:
                parameters[name] = None
            else:
                parameters[name] = param.default

        return parameters

    async def _execute_single_task(self, task_config: Dict):
        """Выполняет асинхронно одну задачу"""
        task_id = task_config["id"]
        operation_name = task_config["operation"]
        dependent_params = task_config["dependent_params"]

        state = await self._load_task_state(task_id)
        all_params = state["params"]
        print(all_params, type(all_params))
        current_retry = state.get("retry_count", 0) if state else 0


        print(f"Запускаем {task_id}...")

        for attempt in range(current_retry, self.max_retries):
            attempt_number = attempt + 1

            try:
                print(f" Запускаем {task_id}... (попытка {attempt_number}/{self.max_retries})")
                if dependent_params:
                    for key, value in dependent_params.items():
                        prev_task_id, result_field, param_name = dependent_params[key].split(".")[0], \
                        dependent_params[key].split(".")[1], dependent_params[key].split(".")[2]
                        if prev_task_id in self.results:
                            if param_name in self.results[prev_task_id]:
                                dependent_params[key] = self.results[prev_task_id][param_name]
                            else:
                                raise ValueError(f"Parametr '{param_name}' not found in task results")
                        else:
                            raise ValueError(f"Task with id '{prev_task_id}' not found in dag config file")


                for key in dependent_params.keys():
                    all_params[key] = dependent_params[key]

                all_params_str = str(all_params)



                # Сохраняем статус running
                await self._save_task_state(
                    task_id,
                    status = "running",
                    params = all_params_str,
                    retry_count=attempt_number
                )


                operation_func = self.operations[operation_name]
                result = await operation_func(**all_params)

                # Успех - сохраняем результат
                await self._save_task_state(
                    task_id,
                    status="completed",
                    params=all_params_str,
                    result=result,
                    retry_count=attempt_number
                )

                self.results[task_id] = result
                print(f"{task_id} завершена")
                print(f"Результаты: {result}\n")
                self.ready_tasks = await self._find_ready_tasks(self.dag_config["tasks"])
                await self._execute_tasks(self.ready_tasks)
                break  # Выходим из цикла retry при успехе

            except Exception as e:
                print(f"{task_id} упала с ошибкой (попытка {attempt_number}/{self.max_retries}): {e}")

                # Сохраняем ошибку
                await self._save_task_state(
                    task_id,
                    status="failed",
                    params = all_params_str,
                    error=str(e),
                    retry_count=attempt_number
                )

                # Проверяем есть ли еще попытки
                if attempt_number < self.max_retries:
                    print(f"Повтор {task_id} через {self.retry_delay}с...")
                    await asyncio.sleep(self.retry_delay)
                else:
                    print(f"{task_id} окончательно упала после {self.max_retries} попыток")
                    # Можно выбросить исключение или просто залогировать
                    break

    async def _save_task_state(self, task_id: str, status: str, params: str, result=None, error=None, retry_count=0):
        """Сохраняет состояние задачи в БД"""
        async with aiosqlite.connect(self.db_path) as db:
            # Проверяем существующую запись для created_at
            async with db.execute(
                    f'SELECT created_at FROM {self.dag_id} WHERE task_id = ?', (task_id,)
            ) as cursor:
                existing = await cursor.fetchone()
                created_at = existing[0] if existing else time.time()

            await db.execute(f'''
                UPDATE {self.dag_id} 
                SET status = ?, result = ?, error = ?, params = ?, retry_count = ?, created_at = ?, updated_at = ?
                WHERE task_id = ?
            ''', (
                status,
                json.dumps(result),
                error,
                json.dumps(params),
                retry_count,
                time.time(),
                time.time(),
                task_id
            ))
            await db.commit()

    async def get_dag_status(self):
        """Возвращает статус всех задач (для мониторинга)"""
        status = {}
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(f'SELECT * FROM {self.dag_id}') as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    status[row[0]] = {
                        "status": row[1],
                        "result": row[2],
                        "error": row[3],
                        "retry_count": row[4],
                        "created_at": row[5],
                        "updated_at": row[6]
                    }
        return status
    def save_all_dag_data_for_user(self):
        print("Созраняем данные в ZIP")
        self.dag_config["dag_id"] = self.dag_id
        print(self.dag_config)
        print(self.results)
        folderpath = f"./dags/{self.dag_id}"
        if not os.path.exists(folderpath):
            os.mkdir(folderpath)
        else:
            for filename in os.listdir(folderpath):
                file_path = os.path.join(folderpath, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
        # Сохраняем конфиг и результаты
        config_path =  os.path.join(folderpath, "config.json")
        with open(config_path, "w", encoding="utf-8") as file:
            json.dump(self.dag_config, file, ensure_ascii=False, indent=4)
        res_path = os.path.join(folderpath, "results.json")
        with open(res_path, "w", encoding="utf-8") as file:
            json.dump(self.results, file, ensure_ascii=False, indent=4)

        for task in self.results.keys():
            for param in self.results[task].keys():
                if param == "output_file_path":
                    source_path = self.results[task][param]
                    name = os.path.basename(source_path)
                    new_path = os.path.join(folderpath, name)
                    os.rename(source_path, new_path)
        print(f"Данные DAG теперь лежат в {folderpath}")




