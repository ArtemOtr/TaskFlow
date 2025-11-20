import asyncio
import json
from orchestrator import TaskOrchestrator
from operations import OPERATIONS
import random


async def main(data_pipeline: str):
    with open(data_pipeline, "r") as f:
        dag_config = json.load(f)
    dag_id = random.randint(1000000, 9999999)
    dag_id = f"dag{dag_id}"
    orchestrator = TaskOrchestrator(
        dag_id = dag_id,
        dag_config=dag_config,
        operations=OPERATIONS,
        max_retries=3,
        retry_delay=2
    )

    print("Запускаем DAG...")


    results = await orchestrator.execute_dag(recovery_mode=False)

    print("\n Финальные результаты выполнения:")
    for task_id, result in results.items():
        print(f"{task_id}: {result}")




if __name__ == "__main__":
    data_config_path = input("Введите путь до конфига: ")
    asyncio.run(main(data_pipeline = data_config_path))