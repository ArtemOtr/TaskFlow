import aiohttp
import asyncio
import json
from typing import Dict, Any
import random
import os


async def fetch_api_data(url: str, method: str, headers: Dict = None, params: Dict = None, output_path: str = None) -> Dict[str, Any]:
    """
    Получает данные через API

    Args:
        url: URL API endpoint
        method: HTTP метод (GET, POST, etc.)
        headers: HTTP заголовки
        params: Параметры запроса

    Returns:
        Словарь с результатами запроса
    """
    if not headers:
        headers = {}
    if not params:
        params = {}

    if not output_path:
        id = random.randint(1000000, 9999999)
        output_path = f"./userdata/{id}.json"
        while os.path.exists(output_path):
            id = random.randint(1000000, 9999999)
            output_path = f"./userdata/{id}.json"

    try:
        async with aiohttp.ClientSession() as session:
            if method.upper() == "GET":
                async with session.get(url, headers=headers, params=params) as response:
                    status_code = response.status
                    text = await response.text()

                    # Пытаемся распарсить JSON, если это возможно
                    try:
                        data = await response.json()
                    except:
                        data = text

            elif method.upper() == "POST":
                async with session.post(url, headers=headers, json=params) as response:
                    status_code = response.status
                    text = await response.text()
                    try:
                        data = await response.json()
                    except:
                        data = text
            else:
                raise ValueError(f"Неподдерживаемый HTTP метод: {method}")

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            return {"output_json_path": output_path}

    except Exception as e:
        raise ConnectionError(f"Request failed: {str(e)}") from e