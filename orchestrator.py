import asyncio
from typing import Dict, List, Any
import aiosqlite
import time

class TaskOrchestrator:
    def __init__(self, dag_config, operations,  db_path = "orchestrator.db", max_retries=3, retry_delay=1):
        self.dag_config = dag_config
        self.results = {}
        self.db_path = db_path
        self._db_initialized = False
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.operations = operations

    async def init_db(self):
        if not self._db_initialized:
            async with aiosqlite.connect(self.db_path) as db:
                # Создаем таблицу
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS task_states (
                        task_id TEXT PRIMARY KEY,
                        status TEXT,
                        result TEXT,
                        error TEXT,
                        retry_count INTEGER,
                        created_at REAL,
                        updated_at REAL
                    )
                ''')

                # Инициализируем все задачи из DAG конфига
                for task in self.dag_config["tasks"]:
                    task_id = task["id"]
                    await db.execute('''
                        INSERT INTO task_states 
                        (task_id, status, retry_count, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        task_id,
                        "pending",
                        0,
                        time.time(),
                        time.time()
                    ))

                await db.commit()
        self._db_initialized = True
    async def cleanup_db(self):
        """Очистка DB"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                DELETE FROM task_states
            ''')
            await db.commit()

    async def _load_task_state(self, task_id: str) -> Dict:
        """Получает состояние операции из БД"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT status, result, retry_count FROM task_states WHERE task_id = ?", (task_id, )) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        "status": row[0],
                        "result": row[1],
                        "retry_count": row[2]
                    }
        return {}
    async def execute_dag(self, recovery_mode = False):
        """Запуск DAG"""

        dag_id =  self.dag_config["dag_id"]
        print(f"Запуск {dag_id}...")

        await self.init_db()

        if not recovery_mode:
            print(f" Новый запуск DAG: {dag_id}...")
            await self.cleanup_db()


        tasks = self.dag_config["tasks"]

        ready_tasks = await self._find_ready_tasks(tasks)
        while ready_tasks:
            # Выполняем готовые задачи
            await self._execute_tasks(ready_tasks)

            # Находим следующие готовые задачи
            ready_tasks = await self._find_ready_tasks(tasks)

        print(f"✅ Весь DAG {dag_id} выполнен!")
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
    async def _execute_single_task(self, task_config: Dict):
        """Выполняет асинхронно одну задачу"""
        task_id = task_config["id"]
        operation_name = task_config["operation"]
        independent_params = task_config["independent_params"]
        dependent_params = task_config["dependent_params"]

        state = await self._load_task_state(task_id)
        current_retry = state["retry_count"]

        if dependent_params:
            for key, value in dependent_params.items():
                prev_task_id, result_field, param_name = dependent_params[key].split(".")[0], dependent_params[key].split(".")[1], dependent_params[key].split(".")[2]
                if prev_task_id in self.results:
                    dependent_params[key] = self.results[prev_task_id][param_name]
                else:
                    raise ValueError(f"Task with id '{prev_task_id}' not found in dag config file")

        all_params = dict()
        for key in independent_params.keys():
            all_params[key] = independent_params[key]
        for key in dependent_params.keys():
            all_params[key] = dependent_params[key]



        print(f"Запускаем {task_id}...")

        for attempt in range(current_retry, self.max_retries):
            attempt_number = attempt + 1

            try:
                print(f" Запускаем {task_id}... (попытка {attempt_number}/{self.max_retries})")

                # Сохраняем статус running
                await self._save_task_state(
                    task_id,
                    "running",
                    retry_count=attempt_number
                )

                # Выполняем операцию
                operation_func = self.operations[operation_name]
                result = await operation_func(**all_params)

                # Успех - сохраняем результат
                await self._save_task_state(
                    task_id,
                    "completed",
                    result=result,
                    retry_count=attempt_number
                )

                self.results[task_id] = result
                print(f"✅ {task_id} завершена")
                print(f"📊 Результаты: {result}\n")
                break  # Выходим из цикла retry при успехе

            except Exception as e:
                print(f"❌ {task_id} упала с ошибкой (попытка {attempt_number}/{self.max_retries}): {e}")

                # Сохраняем ошибку
                await self._save_task_state(
                    task_id,
                    "failed",
                    error=str(e),
                    retry_count=attempt_number
                )

                # Проверяем есть ли еще попытки
                if attempt_number < self.max_retries:
                    print(f"🔄 Повтор {task_id} через {self.retry_delay}с...")
                    await asyncio.sleep(self.retry_delay)
                else:
                    print(f"💥 {task_id} окончательно упала после {self.max_retries} попыток")
                    # Можно выбросить исключение или просто залогировать
                    break

    async def _save_task_state(self, task_id: str, status: str, result=None, error=None, retry_count=0):
        """Сохраняет состояние задачи в БД"""
        async with aiosqlite.connect(self.db_path) as db:
            # Проверяем существующую запись для created_at
            async with db.execute(
                    'SELECT created_at FROM task_states WHERE task_id = ?', (task_id,)
            ) as cursor:
                existing = await cursor.fetchone()
                created_at = existing[0] if existing else time.time()

            await db.execute('''
                   INSERT OR REPLACE INTO task_states 
                   (task_id, status, result, error, retry_count, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
               ''', (
                task_id,
                status,
                str(result) if result else None,
                error,
                retry_count,
                created_at,
                time.time()
            ))
            await db.commit()

    async def get_dag_status(self):
        """Возвращает статус всех задач (для мониторинга)"""
        status = {}
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('SELECT * FROM task_states') as cursor:
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


