import json
import uuid
import asyncio
import aiohttp
import os
from datetime import datetime
from croniter import croniter
from aiogram.types import BufferedInputFile
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, InputFile, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
from flask.cli import load_dotenv

# --------------------
# CONFIG
# --------------------

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_URL = "http://0.0.0.0:5000/api"

DATA_DIR = "./tg_data"
USERS_FILE = f"{DATA_DIR}/users.json"
TMP_DIR = f"{DATA_DIR}/tmp"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

if not os.path.exists(USERS_FILE):
    with open(USERS_FILE, "w") as f:
        json.dump([], f, indent=4)

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


# --------------------
# UTILS
# --------------------

def save_users(users):
    """Сохранение пользователей с конвертацией datetime в строку"""
    # Создаем копию для сериализации
    users_serializable = []

    for user in users:
        user_copy = user.copy()

        # Конвертируем datetime объекты в строки
        if 'last_run' in user_copy and isinstance(user_copy['last_run'], datetime):
            user_copy['last_run'] = user_copy['last_run'].isoformat()

        if 'next_run' in user_copy and isinstance(user_copy['next_run'], datetime):
            user_copy['next_run'] = user_copy['next_run'].isoformat()

        users_serializable.append(user_copy)

    # Сохраняем
    with open('./tg_data/users.json', 'w', encoding='utf-8') as f:
        json.dump(users_serializable, f, ensure_ascii=False, indent=2)


def load_users():
    """Загрузка пользователей с конвертацией строк в datetime"""
    try:
        with open('./tg_data/users.json', 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return []

            users = json.loads(content)

            # Конвертируем строки обратно в datetime
            for user in users:
                if 'last_run' in user and isinstance(user['last_run'], str):
                    user['last_run'] = datetime.fromisoformat(user['last_run'])

                if 'next_run' in user and isinstance(user['next_run'], str):
                    user['next_run'] = datetime.fromisoformat(user['next_run'])

            return users

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        print(f"Ошибка загрузки users: {e}")
        return []

def update_user(chat_id, **kwargs):
    users = load_users()
    for u in users:
        if u["chat_id"] == chat_id:
            for k,v in kwargs.items():
                u[k] = v
            break
    else:
        users.append({
            "chat_id": chat_id,
            "username": kwargs.get("username"),
            "cron": kwargs.get("cron"),
            "method": kwargs.get("method"),
            "config": kwargs.get("config"),
            "last_run": None,
            "next_run": None
        })
    save_users(users)


# --------------------
# FSM
# --------------------

class UserState(StatesGroup):
    waiting_for_config = State()
    menu = State()
    waiting_for_cron = State()
    waiting_for_method = State()


# --------------------
# UI
# --------------------

def menu_keyboard(config_set, cron_set, method_set):
    btns = [
        [InlineKeyboardButton(text="Загрузить config", callback_data="set_config")],
        [InlineKeyboardButton(text="Cron", callback_data="set_cron")],
        [InlineKeyboardButton(text="Метод получения", callback_data="set_method")],
    ]
    if config_set and method_set:
        btns.append([InlineKeyboardButton(text="Готово", callback_data="finish")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def method_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Web-интерфейс", callback_data="method_web")],
            [InlineKeyboardButton(text="ZIP архив", callback_data="method_zip")],
        ]
    )

def settings_text(data):
    return (
        "Ваши настройки:\n"
        f"Config: {'config.json' if data.get('config') else '-'}\n"
        f"Cron: {data.get('cron') or '-'}\n"
        f"Метод получения: {data.get('method') or '-'}"
    )


# --------------------
# START
# --------------------

@dp.message(Command("start"))
async def start_cmd(msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(UserState.waiting_for_config)
    await msg.answer("Привет! 👋\nСкинь конфиг в формате JSON.")


# --------------------
# CONFIG
# --------------------

@dp.message(UserState.waiting_for_config)
async def receive_config(msg: Message, state: FSMContext):
    if not msg.document or not msg.document.file_name.endswith(".json"):
        await msg.answer("Отправь config.json")
        return

    file = await bot.get_file(msg.document.file_id)
    content = await bot.download_file(file.file_path)

    try:
        config = json.loads(content.read())
    except:
        await msg.answer("Невалидный JSON.")
        return

    await state.update_data(config=config, cron=None, method=None)

    data = await state.get_data()
    await state.set_state(UserState.menu)
    await msg.answer(
        settings_text(data),
        reply_markup=menu_keyboard(True, False, False)
    )


# --------------------
# CRON
# --------------------

@dp.callback_query(F.data == "set_cron", UserState.menu)
async def ask_cron(cb: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_for_cron)
    await cb.message.answer("Введи cron-выражение:")
    await cb.answer()

@dp.message(UserState.waiting_for_cron)
async def set_cron(msg: Message, state: FSMContext):
    cron = msg.text.strip()
    await state.update_data(cron=cron)

    data = await state.get_data()
    await state.set_state(UserState.menu)
    await msg.answer(
        settings_text(data),
        reply_markup=menu_keyboard(True, True, data.get("method") is not None)
    )


# --------------------
# METHOD
# --------------------

@dp.callback_query(F.data == "set_method", UserState.menu)
async def choose_method(cb: CallbackQuery, state: FSMContext):
    await state.set_state(UserState.waiting_for_method)
    await cb.message.answer("Выбери метод:", reply_markup=method_keyboard())
    await cb.answer()


@dp.callback_query(F.data.in_(["method_web", "method_zip"]), UserState.waiting_for_method)
async def set_method(cb: CallbackQuery, state: FSMContext):
    method = "web" if cb.data == "method_web" else "zip"
    await state.update_data(method=method)

    data = await state.get_data()
    await state.set_state(UserState.menu)
    await cb.message.answer(
        settings_text(data),
        reply_markup=menu_keyboard(True, data.get("cron") is not None, True)
    )
    await cb.answer()


# --------------------
# FINISH
# --------------------

@dp.callback_query(F.data == "finish", UserState.menu)
async def finish(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()

    if not data.get("config") or not data.get("method"):
        await cb.answer("Заполни обязательные поля!", show_alert=True)
        return

    # сохраняем юзера
    update_user(
        chat_id=cb.message.chat.id,
        username=cb.message.chat.username,
        config=data["config"],
        cron=data.get("cron"),
        method=data["method"]
    )

    # единоразовый запуск (если нет cron)
    if not data.get("cron"):
        await perform_api_action(cb.message.chat.id)

        await cb.message.answer("Класс 😎\nЗапуск выполнен сейчас!")
    else:
        await cb.message.answer(f"Класс 😎\nПервый запуск будет по cron: {data['cron']}")

    await cb.answer()


# --------------------
# API ACTION
# --------------------

async def perform_api_action(chat_id):
    """Вызывает API и отправляет данные пользователю."""
    users = load_users()
    user = next(u for u in users if u["chat_id"] == chat_id)

    async with aiohttp.ClientSession() as session:
        if user["method"] == "web":
            async with session.post(API_URL + "/web", json=user["config"]) as resp:
                    j = await resp.json()
                    link = j.get("link")
                    await bot.send_message(chat_id, f"Ваши данные готовы:\n{link}")

        else:  # zip
            try:
                users = load_users()
                user = next(u for u in users if u["chat_id"] == chat_id)

                async with aiohttp.ClientSession() as session:
                    async with session.post(
                            API_URL + "/cli",
                            headers={"Content-Type": "application/json"},
                            json=user["config"]
                    ) as resp:
                        if resp.status == 200:
                            file_bytes = await resp.read()

                            # Создаем BufferedInputFile
                            input_file = BufferedInputFile(
                                file=file_bytes,
                                filename=f"archive_{chat_id}.zip"
                            )

                            await bot.send_document(
                                chat_id=chat_id,
                                document=input_file,
                                caption="Ваш ZIP архив"
                            )

                            print(f"✅ Отправил архив пользователю {chat_id}")

            except Exception as e:
                print(f"❌ Ошибка: {e}")


# --------------------
# CRON CHECKER
# --------------------

async def cron_worker():
    while True:
        users = load_users()
        now = datetime.now()

        for user in users:
            if not user.get("cron"):
                continue

            cron = user["cron"]
            if not user.get("next_run"):
                itr = croniter(cron, user.get("last_run") or now)
                user["next_run"] = itr.get_next(datetime)
                save_users(users)


            if user["next_run"] <= now:
                await perform_api_action(user["chat_id"])
                user["last_run"] = now
                itr = croniter(cron, user.get("last_run") or now)
                user["next_run"] = itr.get_next(datetime)
                save_users(users)

        await asyncio.sleep(20)

# --------------------
# RUN
# --------------------

async def main():
    asyncio.create_task(cron_worker())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
