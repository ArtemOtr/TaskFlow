import aiohttp
import asyncio
from typing import Dict, Any
import os
import json

class TelegramBot:
    def __init__(self, token: str):
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"


async def get_chat_id_by_username(username: str,  token: str) -> int:

    bot = TelegramBot(token=token)
    if username[0] == "@":
        username = username[1:]
    users = {}
    if os.path.exists("./tg_data/tg_user_ids.json"):
        with open("./tg_data/tg_user_ids.json", 'r', encoding='utf-8') as file:
            users = json.load(file)
        if username in users:
            chat_id = users[username]
            return chat_id
    try:
        async with aiohttp.ClientSession() as session:
            response = await session.get(bot.base_url + "/getUpdates")
            result = await response.json()

            if not result["ok"]:
                raise ConnectionError(f"Ошибка работы Telegram API: {result}")

            for update in result["result"]:
                if "message" in update and "from" in update["message"]:
                    update_username = update["message"]["from"]["username"]

                    if update_username == username:
                        chat_id = update["message"]["from"]["id"]
                        print(f"Найден chat_id: {chat_id}")
                        users[username] = chat_id
                        with open("tg_user_ids.json", "w", encoding="utf-8") as file:
                            json.dump(users, file, ensure_ascii=False, indent=4)
                        return chat_id

            raise ValueError(f"Пользователь @{username} не найден в истории обновлений")
    except ValueError:
        raise ValueError(f"Пользователь @{username} не найден в истории обновлений")





async def send_telegram_message(username: str, message: str, token: str) -> Dict[str, Any]:

    bot = TelegramBot(token=token)
    chat_id = await get_chat_id_by_username(username=username, token = token)
    data =  {"chat_id": chat_id, "text": message}

    try:
        async with aiohttp.ClientSession() as session:
            response = await session.post(bot.base_url + "/sendMessage", json=data)
            result = await response.json()
            if not result["ok"]:
                raise ConnectionError(f"Ошибка работы Telegram API {result}")
            return {"tg_api_response": result}
    except Exception as e:
        print(f"Ошибка при отправки сообщения: {e}")


