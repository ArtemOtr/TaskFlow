import asyncio
import json
import yaml
import csv
import pandas as pd
from typing import Dict, Any, List
import io

async def dict_to_string(data: Dict) -> Dict[str, Any]:
    """
    Конвертирует словарь в читаемую строку

    Args:
        data: словарь для конвертации
        separator: разделитель между ключ-значение
    """
    print(f"Конвертируем словарь в строку...")

    try:
        result = str(data)

        print(f"Словарь конвертирован в строку")
        return {"string" :result}

    except Exception as e:
        error_result = {
            "success": False,
            "error": str(e),
            "original_data": str(data)[:200] + "..." if len(str(data)) > 200 else str(data)
        }
        raise Exception(f"Ошибка выполнения операции: {error_result}")


async def json_to_string(data: str) -> str:
    """
    Конвертирует JSON-объект (словарь) в читаемую строку

    Args:
        data: JSON-объект (словарь) для конвертации
    """
    print(f"Конвертируем JSON в строку...")

    try:
        with open(data, 'r', encoding='utf-8') as file:
            result = str(json.load(file))

        print(f"JSON конвертирован в строку")
        return {"string": result}

    except Exception as e:
        error_result = {
            "success": False,
            "error": str(e),
            "original_data": str(data)[:200] + "..." if len(str(data)) > 200 else str(data)
        }
        raise Exception(f"Ошибка выполнения операции: {error_result}")

async def async_sleep(sleep_time: int = 10):
    await asyncio.sleep(sleep_time)
    return {"sleep_succesfull" : True}