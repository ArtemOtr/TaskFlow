# main.py
import asyncio
import json
from orchestrator import TaskOrchestrator
from operations import OPERATIONS


async def main(data_pipeline: str, ):
    # Загружаем конфиг DAG
    with open(data_pipeline, "r") as f:
        dag_config = json.load(f)

    # Создаем оркестратор
    orchestrator = TaskOrchestrator(
        dag_config=dag_config,
        operations=OPERATIONS,
        max_retries=3,
        retry_delay=2
    )

    print("Запускаем DAG...")

    # Запускаем DAG
    try:
        results = await orchestrator.execute_dag(recovery_mode=False)

        print("\n Финальные результаты выполнения:")
        for task_id, result in results.items():
            print(f"{task_id}: {result}")

    except Exception as e:
        print(f"Критическая ошибка: {e}")

        # Отправляем alert об ошибке
        try:
            from operations.telegram_ops import send_telegram_alert
            await send_telegram_alert(f" Пайплайн упал с ошибкой: {e}")
        except:
            print("Не удалось отправить alert в Telegram")


if __name__ == "__main__":
    asyncio.run(main())