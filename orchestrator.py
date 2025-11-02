import asyncio
from typing import Dict, List, Any

class TaskOrchestrator:
    def __init__(self, dag_config):
        self.dag_config = dag_config
        self.results = {}

    async def execute_dag(self):
        """Запуск DAG"""

        dag_id =  self.dag_config["dag_id"]
        print(f"Запуск {dag_id}...")

        tasks = self.dag_config["tasks"]

        # Находим все задачи, готовые к выполнению (без зависимостей)
        ready_tasks = [task for task in tasks if not task["dependencies"]]

        while ready_tasks:
            # Выполняем готовые задачи параллельно
            await self._execute_tasks(ready_tasks)

            # Находим след готовые задачи
            ready_tasks = []
            for task in tasks:
                # Если задача еще не выполнена и все её зависимости выполнены
                if (task["id"] not in self.results and
                        all(dep in self.results for dep in task["dependencies"])):
                    ready_tasks.append(task)

        print(f"✅ Весь DAG {dag_id} выполнен!")
        return self.results

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


        # Выполняем операцию
        operation_func = self.operations[operation_name]
        result = await operation_func(**all_params)
        # Все операции возвращают значения в виде словаря


        self.results[task_id] = result
        print(f"✅ {task_id} завершена")
        print(f"Результаты задачи: \n {result}\n\n")

