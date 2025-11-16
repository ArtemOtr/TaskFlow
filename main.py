import asyncio
import json
from orchestrator import TaskOrchestrator
from operations import OPERATIONS


async def main(data_pipeline: str):
    with open(data_pipeline, "r") as f:
        dag_config = json.load(f)

    orchestrator = TaskOrchestrator(
        dag_config=dag_config,
        operations=OPERATIONS,
        max_retries=3,
        retry_delay=2
    )

    print("Запускаем DAG...")

    try:
        results = await orchestrator.execute_dag(recovery_mode=False)

        print("\n Финальные результаты выполнения:")
        for task_id, result in results.items():
            print(f"{task_id}: {result}")

    except Exception as e:
        print(f"Критическая ошибка: {e}")


if __name__ == "__main__":
    data_config_path = input("Введите путь до конфига: ")
    asyncio.run(main(data_pipeline = data_config_path))