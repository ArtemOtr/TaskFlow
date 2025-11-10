import aiohttp
import asyncio
from typing import Dict, Any
import os

class TelegramBot:
    def __init__(self, token: str):
        self.token = token
        self.base_url = f"https://api.telegram.org/bot{token}"

    async def get_chat_id(self, username: str) -> int:
        """Получает chat_id по username"""
        if not username.startswith("@"):
            username = f"@{username}"

        url = f"{self.base_url}/getChat"
        payload = {"chat_id": username}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=payload) as response:
                    data = await response.json()
                    if response.status == 200 and data.get("ok"):
                        chat_id = data["result"]["id"]
                        print(f"✅ Найден chat_id для {username}: {chat_id}")
                        return chat_id
                    else:
                        raise Exception(f"Ошибка Telegram API: {data}")
        except Exception as e:
            raise Exception(f"Не удалось получить chat_id для {username}: {e}")

    async def send_message(
        self, username: str, text: str, parse_mode: str = "HTML"
    ) -> Dict[str, Any]:
        """Отправляет сообщение пользователю по username"""
        chat_id = await self.get_chat_id(username)
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        return {"success": True, "username": username, "text": text}
                    else:
                        error_text = await response.text()
                        raise Exception(f"Ошибка Telegram API: {error_text}")
        except Exception as e:
            raise Exception(f"Ошибка при отправке сообщения {username}: {e}")


token = os.environ.get("TELEGRAM_BOT_TOKEN")
if not token:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set in environment")

bot = TelegramBot(token=token)

async def send_telegram_message(username: str, message: str) -> Dict[str, Any]:
    return await bot.send_message(username, message)


# Для теста:
# asyncio.run(send_telegram_message("username_without_@", "Привет!"))
