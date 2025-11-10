import aiohttp
import asyncio
import json
from typing import Dict, Any


async def fetch_api_data(url: str, method: str, headers: Dict = None, params: Dict = None) -> Dict[str, Any]:
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

            return {"response": data}
    except Exception as e:
        raise ConnectionError(f"Request failed: {str(e)}") from e