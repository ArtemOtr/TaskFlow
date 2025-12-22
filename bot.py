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

import logging

logger = logging.getLogger("taskflow")
logger.setLevel(logging.INFO)

if not logger.hasHandlers():
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# --------------------
# CONFIG
# --------------------

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_URL = "http://0.0.0.0:5000/api"

DATA_DIR = "./tg_data"
GRAPHS_FILE = f"{DATA_DIR}/graphs.json"
TMP_DIR = f"{DATA_DIR}/tmp"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)

if not os.path.exists(GRAPHS_FILE):
    with open(GRAPHS_FILE, "w") as f:
        json.dump([], f, indent=4)

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


# --------------------
# UTILS
# --------------------

def save_graphs(graphs):
    """Сохранение графов с конвертацией datetime в строку"""
    graphs_serializable = []

    for graph in graphs:
        graph_copy = graph.copy()

        if 'last_run' in graph_copy and isinstance(graph_copy['last_run'], datetime):
            graph_copy['last_run'] = graph_copy['last_run'].isoformat()

        if 'next_run' in graph_copy and isinstance(graph_copy['next_run'], datetime):
            graph_copy['next_run'] = graph_copy['next_run'].isoformat()

        if 'created_at' in graph_copy and isinstance(graph_copy['created_at'], datetime):
            graph_copy['created_at'] = graph_copy['created_at'].isoformat()

        graphs_serializable.append(graph_copy)

    with open(GRAPHS_FILE, 'w', encoding='utf-8') as f:
        json.dump(graphs_serializable, f, ensure_ascii=False, indent=2)

    logger.info(f"Сохранено {len(graphs)} графов в файл")


def load_graphs():
    """Загрузка графов с конвертацией строк в datetime"""
    try:
        with open(GRAPHS_FILE, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                logger.info("Файл graphs.json пуст")
                return []

            graphs = json.loads(content)

            for graph in graphs:
                if 'last_run' in graph and isinstance(graph['last_run'], str):
                    try:
                        graph['last_run'] = datetime.fromisoformat(graph['last_run'])
                    except Exception as e:
                        logger.warning(f"Ошибка преобразования last_run для графа {graph.get('graph_id')}: {e}")
                        graph['last_run'] = None

                if 'next_run' in graph and isinstance(graph['next_run'], str):
                    try:
                        graph['next_run'] = datetime.fromisoformat(graph['next_run'])
                    except Exception as e:
                        logger.warning(f"Ошибка преобразования next_run для графа {graph.get('graph_id')}: {e}")
                        graph['next_run'] = None

                if 'created_at' in graph and isinstance(graph['created_at'], str):
                    try:
                        graph['created_at'] = datetime.fromisoformat(graph['created_at'])
                    except Exception as e:
                        logger.warning(f"Ошибка преобразования created_at для графа {graph.get('graph_id')}: {e}")
                        graph['created_at'] = datetime.now()

            logger.info(f"Загружено {len(graphs)} графов из файла")
            return graphs

    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.error(f"Ошибка загрузки graphs: {e}")
        return []


def get_user_graphs(chat_id):
    """Получить все графы пользователя"""
    graphs = load_graphs()
    user_graphs = [g for g in graphs if g["chat_id"] == chat_id]
    logger.debug(f"Найдено {len(user_graphs)} графов для пользователя {chat_id}")
    return user_graphs


def get_graph_by_id(graph_id):
    """Получить граф по ID"""
    graphs = load_graphs()
    for graph in graphs:
        if graph["graph_id"] == graph_id:
            logger.debug(f"Найден граф {graph_id} - {graph.get('name')}")
            return graph
    logger.warning(f"Граф {graph_id} не найден")
    return None


def create_graph(chat_id, username, config, cron=None, method="web", name=None, is_active=True):
    """Создать новый граф"""
    graphs = load_graphs()

    graph_id = str(uuid.uuid4())
    name = name or f"Граф_{len(get_user_graphs(chat_id)) + 1}"

    new_graph = {
        "graph_id": graph_id,
        "chat_id": chat_id,
        "username": username,
        "name": name,
        "config": config,
        "cron": cron,
        "method": method,
        "is_active": is_active,
        "last_run": None,
        "next_run": None,
        "created_at": datetime.now()
    }

    graphs.append(new_graph)
    save_graphs(graphs)

    logger.info(f"Создан новый граф: ID={graph_id}, имя='{name}', пользователь={username}, cron={cron}, метод={method}")
    return graph_id


def update_graph(graph_id, **kwargs):
    """Обновить граф"""
    graphs = load_graphs()
    updated = False

    for graph in graphs:
        if graph["graph_id"] == graph_id:
            # Логируем изменения
            changes = []
            for key, value in kwargs.items():
                old_value = graph.get(key)
                if old_value != value:
                    changes.append(f"{key}: {old_value} -> {value}")
                graph[key] = value

            # Сбрасываем cron-времена при изменении cron
            if 'cron' in kwargs:
                graph['last_run'] = None
                graph['next_run'] = None
                changes.append("сброшены времена запусков из-за изменения cron")

            if changes:
                logger.info(f"Обновлен граф {graph_id} ({graph.get('name')}): {', '.join(changes)}")
                updated = True

            save_graphs(graphs)
            return updated

    logger.warning(f"Попытка обновления несуществующего графа {graph_id}")
    return False


def delete_graph(graph_id):
    """Удалить граф"""
    graphs = load_graphs()
    initial_count = len(graphs)
    graphs = [g for g in graphs if g["graph_id"] != graph_id]

    if len(graphs) < initial_count:
        save_graphs(graphs)
        logger.info(f"Удален граф {graph_id}")
        return True
    else:
        logger.warning(f"Граф {graph_id} не найден для удаления")
        return False


def toggle_graph_active(graph_id):
    """Включить/выключить граф"""
    graphs = load_graphs()

    for graph in graphs:
        if graph["graph_id"] == graph_id:
            old_status = graph["is_active"]
            graph["is_active"] = not graph["is_active"]
            save_graphs(graphs)

            status_text = "активирован" if graph["is_active"] else "остановлен"
            logger.info(
                f"Граф {graph_id} ({graph.get('name')}) {status_text} (был: {'активен' if old_status else 'остановлен'})")
            return graph["is_active"]

    logger.warning(f"Граф {graph_id} не найден для переключения статуса")
    return None


# --------------------
# FSM
# --------------------

class GraphState(StatesGroup):
    waiting_for_config = State()
    waiting_for_name = State()
    waiting_for_cron = State()
    waiting_for_method = State()
    managing_graphs = State()


# --------------------
# UI
# --------------------

def menu_keyboard(config_set, cron_set, method_set):
    btns = [
        [InlineKeyboardButton(text="📁 Загрузить config", callback_data="set_config")],
        [InlineKeyboardButton(text="🔄 Cron", callback_data="set_cron")],
        [InlineKeyboardButton(text="📤 Метод получения", callback_data="set_method")],
    ]
    if config_set and method_set:
        btns.append([InlineKeyboardButton(text="✅ Готово", callback_data="finish")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def method_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🌐 Web-интерфейс", callback_data="method_web")],
            [InlineKeyboardButton(text="📦 ZIP архив", callback_data="method_zip")],
        ]
    )


def graphs_list_keyboard(graphs):
    """Клавиатура для списка графов"""
    buttons = []

    for graph in graphs:
        status = "✅" if graph.get("is_active", True) else "⏸️"
        btn_text = f"{status} {graph.get('name', 'Без имени')}"

        buttons.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"graph_{graph['graph_id']}"
            )
        ])

    buttons.append([
        InlineKeyboardButton(text="➕ Создать новый", callback_data="create_new")
    ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def graph_detail_keyboard(graph_id, is_active):
    """Клавиатура для управления конкретным графом"""
    status_text = "⏸️ Остановить" if is_active else "▶️ Возобновить"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=status_text, callback_data=f"toggle_{graph_id}"),
                InlineKeyboardButton(text="🚀 Запустить сейчас", callback_data=f"run_now_{graph_id}")
            ],
            [
                InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"rename_{graph_id}"),
                InlineKeyboardButton(text="⚙️ Редактировать", callback_data=f"edit_{graph_id}")
            ],
            [
                InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_{graph_id}"),
                InlineKeyboardButton(text="📊 Статус", callback_data=f"status_{graph_id}")
            ],
            [
                InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_list")
            ]
        ]
    )


def settings_text(data):
    return (
        "Ваши настройки графа:\n\n"
        f"📝 Название: {data.get('name', 'Не задано')}\n"
        f"📁 Config: {'загружен' if data.get('config') else 'нет'}\n"
        f"🔄 Cron: {data.get('cron') or 'не задан'}\n"
        f"📤 Метод получения: {data.get('method') or 'не выбран'}"
    )


# --------------------
# START & MANAGE GRAPHS
# --------------------

@dp.message(Command("start"))
async def start_cmd(msg: Message, state: FSMContext):
    await state.clear()
    logger.info(f"Команда /start от пользователя {msg.from_user.id} (@{msg.from_user.username})")
    await msg.answer(
        "👋 Привет! Я бот для управления графами.\n\n"
        "Используйте команды:\n"
        "/graphs - управление вашими графами\n"
        "/new - создать новый граф\n"
        "/help - справка"
    )


@dp.message(Command("graphs"))
async def list_graphs_cmd(msg: Message, state: FSMContext):
    await state.clear()
    logger.info(f"Команда /graphs от пользователя {msg.from_user.id}")
    graphs = get_user_graphs(msg.chat.id)

    if not graphs:
        logger.info(f"У пользователя {msg.from_user.id} нет графов")
        await msg.answer(
            "📭 У вас пока нет графов.\n\n"
            "Чтобы создать первый граф, используйте команду /new"
        )
        return

    logger.info(f"Пользователь {msg.from_user.id} имеет {len(graphs)} графов")
    await state.set_state(GraphState.managing_graphs)
    await msg.answer(
        "📋 Ваши графы:\n"
        "✅ - активен, ⏸️ - остановлен\n\n"
        "Выберите граф для управления:",
        reply_markup=graphs_list_keyboard(graphs)
    )


@dp.message(Command("new"))
async def new_graph_cmd(msg: Message, state: FSMContext):
    await state.clear()
    logger.info(f"Команда /new от пользователя {msg.from_user.id}")
    await state.set_state(GraphState.waiting_for_name)
    await msg.answer("Дайте имя новому графу:")


@dp.message(Command("help"))
async def help_cmd(msg: Message):
    logger.info(f"Команда /help от пользователя {msg.from_user.id}")
    help_text = """
🤖 **Помощь по боту:**

**Основные команды:**
/graphs - список ваших графов
/new - создать новый граф
/help - эта справка

**Управление графами:**
• ✅ - граф активен и работает по расписанию
• ⏸️ - граф остановлен
• Можно иметь несколько графов с разными настройками
• Каждый граф имеет уникальный config и cron

**Создание графа:**
1. Задайте имя графа
2. Загрузите config.json
3. Настройте cron (опционально)
4. Выберите метод получения данных

**Что такое граф?**
Граф - это отдельная задача с:
- Своим конфигом
- Своим расписанием (cron)
- Своим методом получения результатов
"""
    await msg.answer(help_text)


# --------------------
# GRAPH CREATION FLOW
# --------------------

@dp.message(GraphState.waiting_for_name)
async def receive_graph_name(msg: Message, state: FSMContext):
    name = msg.text.strip()
    if len(name) > 50:
        logger.warning(f"Пользователь {msg.from_user.id} отправил слишком длинное имя: {name}")
        await msg.answer("Имя слишком длинное. Максимум 50 символов.")
        return

    logger.info(f"Пользователь {msg.from_user.id} задал имя графа: {name}")
    await state.update_data(name=name)
    await state.set_state(GraphState.waiting_for_config)
    await msg.answer(f"Имя графа: {name}\n\nТеперь отправьте config.json файл:")


@dp.message(GraphState.waiting_for_config)
async def receive_config(msg: Message, state: FSMContext):
    if not msg.document or not msg.document.file_name.endswith(".json"):
        logger.warning(f"Пользователь {msg.from_user.id} отправил не JSON файл")
        await msg.answer("Отправьте config.json файл")
        return

    logger.info(f"Пользователь {msg.from_user.id} отправил файл: {msg.document.file_name}")
    file = await bot.get_file(msg.document.file_id)
    content = await bot.download_file(file.file_path)

    try:
        config = json.loads(content.read())
        logger.info(f"Config успешно загружен, размер: {len(str(config))} символов")
    except Exception as e:
        logger.error(f"Ошибка парсинга JSON от пользователя {msg.from_user.id}: {e}")
        await msg.answer("Невалидный JSON.")
        return

    await state.update_data(config=config)

    data = await state.get_data()
    await state.set_state(GraphState.waiting_for_method)
    await msg.answer(
        f"Config загружен!\n\n"
        f"Название: {data['name']}\n\n"
        f"Теперь выберите метод получения результатов:",
        reply_markup=method_keyboard()
    )


@dp.callback_query(F.data.in_(["method_web", "method_zip"]), GraphState.waiting_for_method)
async def set_method(cb: CallbackQuery, state: FSMContext):
    method = "web" if cb.data == "method_web" else "zip"
    logger.info(f"Пользователь {cb.from_user.id} выбрал метод: {method}")
    await state.update_data(method=method)

    data = await state.get_data()
    await state.set_state(GraphState.waiting_for_cron)
    await cb.message.answer(
        f"Метод: {'Web-интерфейс' if method == 'web' else 'ZIP архив'}\n\n"
        f"Теперь введите cron-выражение для автоматического запуска.\n"
        f"Если не нужен автоматический запуск, отправьте 'нет' или пропустите, отправив любое сообщение."
    )
    await cb.answer()


@dp.message(GraphState.waiting_for_cron)
async def set_cron_final(msg: Message, state: FSMContext):
    cron = msg.text.strip()
    logger.info(f"Пользователь {msg.from_user.id} ввел cron: '{cron}'")

    if cron.lower() in ['нет', 'no', 'skip', 'пропустить']:
        cron = None
        logger.info("Пользователь отключил автоматический запуск")
    elif cron:
        try:
            croniter(cron, datetime.now())
            logger.info(f"Cron выражение валидно: {cron}")
        except Exception as e:
            logger.warning(f"Некорректное cron выражение от пользователя {msg.from_user.id}: {cron}, ошибка: {e}")
            await msg.answer("Неверное cron-выражение. Попробуйте снова:")
            return

    await state.update_data(cron=cron)

    data = await state.get_data()

    graph_id = create_graph(
        chat_id=msg.chat.id,
        username=msg.chat.username,
        name=data['name'],
        config=data['config'],
        cron=data.get('cron'),
        method=data['method'],
        is_active=True
    )

    await state.clear()

    if cron:
        logger.info(f"Запускаю первый автоматический запуск для графа {graph_id}")
        await perform_api_action(graph_id)

    response_text = (
        f"✅ Граф '{data['name']}' успешно создан!\n\n"
        f"📝 Название: {data['name']}\n"
        f"📁 Config: загружен\n"
        f"📤 Метод: {'Web-интерфейс' if data['method'] == 'web' else 'ZIP архив'}\n"
        f"🔄 Cron: {cron if cron else 'ручной запуск'}\n"
        f"🔧 Статус: активен\n\n"
    )

    if cron:
        response_text += f"Первый автоматический запуск будет по расписанию."
    else:
        response_text += "Для запуска перейдите в управление графами (/graphs) и нажмите 'Запустить сейчас'."

    await msg.answer(response_text)


# --------------------
# GRAPH MANAGEMENT
# --------------------

@dp.callback_query(F.data == "back_to_list", GraphState.managing_graphs)
async def back_to_list(cb: CallbackQuery, state: FSMContext):
    logger.info(f"Пользователь {cb.from_user.id} вернулся к списку графов")
    graphs = get_user_graphs(cb.message.chat.id)

    if not graphs:
        await cb.message.answer("У вас нет графов. Используйте /new для создания.")
        await state.clear()
        return

    await cb.message.edit_text(
        "📋 Ваши графы:\n"
        "✅ - активен, ⏸️ - остановлен\n\n"
        "Выберите граф для управления:",
        reply_markup=graphs_list_keyboard(graphs)
    )
    await cb.answer()


@dp.callback_query(F.data == "create_new", GraphState.managing_graphs)
async def create_new_from_list(cb: CallbackQuery, state: FSMContext):
    logger.info(f"Пользователь {cb.from_user.id} создает новый граф из списка")
    await state.set_state(GraphState.waiting_for_name)
    await cb.message.answer("Дайте имя новому графу:")
    await cb.answer()


@dp.callback_query(F.data.startswith("graph_"), GraphState.managing_graphs)
async def show_graph_detail(cb: CallbackQuery, state: FSMContext):
    graph_id = cb.data.split("_")[1]
    logger.info(f"Пользователь {cb.from_user.id} запросил детали графа {graph_id}")
    graph = get_graph_by_id(graph_id)

    if not graph:
        logger.warning(f"Граф {graph_id} не найден для пользователя {cb.from_user.id}")
        await cb.answer("Граф не найден!")
        return

    status_text = "✅ Активен" if graph.get("is_active", True) else "⏸️ Остановлен"
    cron_text = graph.get("cron") or "Ручной запуск"
    last_run = graph.get("last_run")
    last_run_text = last_run.strftime("%d.%m.%Y %H:%M") if last_run else "Никогда"

    detail_text = (
        f"📋 Детали графа:\n\n"
        f"📝 Название: {graph.get('name')}\n"
        f"🔧 Статус: {status_text}\n"
        f"🔄 Cron: {cron_text}\n"
        f"📤 Метод: {'Web-интерфейс' if graph.get('method') == 'web' else 'ZIP архив'}\n"
        f"⏰ Последний запуск: {last_run_text}\n"
        f"🆔 ID: {graph_id[:8]}..."
    )

    await cb.message.edit_text(
        detail_text,
        reply_markup=graph_detail_keyboard(graph_id, graph.get("is_active", True))
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("toggle_"), GraphState.managing_graphs)
async def toggle_graph(cb: CallbackQuery):
    graph_id = cb.data.split("_")[1]
    logger.info(f"Пользователь {cb.from_user.id} переключает статус графа {graph_id}")
    new_status = toggle_graph_active(graph_id)

    if new_status is not None:
        status_text = "активирован" if new_status else "остановлен"
        graph = get_graph_by_id(graph_id)

        await cb.message.edit_text(
            f"Граф '{graph.get('name')}' {status_text}!\n\n"
            f"Текущий статус: {'✅ Активен' if new_status else '⏸️ Остановлен'}",
            reply_markup=graph_detail_keyboard(graph_id, new_status)
        )
    else:
        logger.error(f"Не удалось переключить статус графа {graph_id}")
        await cb.answer("Граф не найден!", show_alert=True)

    await cb.answer()


@dp.callback_query(F.data.startswith("run_now_"), GraphState.managing_graphs)
async def run_graph_now(cb: CallbackQuery):
    graph_id = cb.data.split("_")[2]
    logger.info(f"Пользователь {cb.from_user.id} запускает граф {graph_id} вручную")
    graph = get_graph_by_id(graph_id)

    if not graph:
        logger.error(f"Граф {graph_id} не найден для ручного запуска")
        await cb.answer("Граф не найден!", show_alert=True)
        return

    logger.info(f"Ручной запуск графа {graph_id} ({graph.get('name')})")
    await cb.answer("Запускаю граф...")
    await cb.message.answer(f"🚀 Запускаю граф '{graph.get('name')}'...")

    success = await perform_api_action(graph_id)

    if success:
        logger.info(f"Ручной запуск графа {graph_id} успешен")
        await cb.message.answer(f"✅ Граф '{graph.get('name')}' успешно выполнен!")
    else:
        logger.error(f"Ошибка при ручном запуске графа {graph_id}")
        await cb.message.answer(f"❌ Ошибка при выполнении графа '{graph.get('name')}'")


@dp.callback_query(F.data.startswith("delete_"), GraphState.managing_graphs)
async def delete_graph_handler(cb: CallbackQuery):
    graph_id = cb.data.split("_")[1]
    logger.info(f"Пользователь {cb.from_user.id} удаляет граф {graph_id}")
    graph = get_graph_by_id(graph_id)

    if not graph:
        logger.error(f"Граф {graph_id} не найден для удаления")
        await cb.answer("Граф не найден!", show_alert=True)
        return

    delete_graph(graph_id)
    logger.info(f"Граф {graph_id} ({graph.get('name')}) удален пользователем {cb.from_user.id}")

    graphs = get_user_graphs(cb.message.chat.id)

    if not graphs:
        await cb.message.edit_text(
            f"🗑️ Граф '{graph.get('name')}' удален!\n\n"
            "У вас больше нет графов. Используйте /new для создания нового."
        )
    else:
        await cb.message.edit_text(
            f"🗑️ Граф '{graph.get('name')}' удален!\n\n"
            "Выберите граф для управления:",
            reply_markup=graphs_list_keyboard(graphs)
        )

    await cb.answer()


@dp.callback_query(F.data.startswith("status_"), GraphState.managing_graphs)
async def show_graph_status(cb: CallbackQuery):
    graph_id = cb.data.split("_")[1]
    logger.info(f"Пользователь {cb.from_user.id} запросил статус графа {graph_id}")
    graph = get_graph_by_id(graph_id)

    if not graph:
        logger.error(f"Граф {graph_id} не найден для показа статуса")
        await cb.answer("Граф не найден!", show_alert=True)
        return

    status_text = "✅ Активен" if graph.get("is_active", True) else "⏸️ Остановлен"
    cron_text = graph.get("cron") or "Ручной запуск"
    last_run = graph.get("last_run")
    last_run_text = last_run.strftime("%d.%m.%Y %H:%M:%S") if last_run else "Никогда"
    next_run = graph.get("next_run")
    next_run_text = next_run.strftime("%d.%m.%Y %H:%M:%S") if next_run else "Не запланирован"
    created_at = graph.get("created_at", datetime.now())
    created_text = created_at.strftime("%d.%m.%Y %H:%M") if isinstance(created_at, datetime) else "Неизвестно"

    status_message = (
        f"📊 Статус графа: {graph.get('name')}\n\n"
        f"🔧 Состояние: {status_text}\n"
        f"🔄 Расписание: {cron_text}\n"
        f"⏰ Последний запуск: {last_run_text}\n"
        f"⏳ Следующий запуск: {next_run_text}\n"
        f"📅 Создан: {created_text}\n"
        f"📤 Метод: {'Web-интерфейс' if graph.get('method') == 'web' else 'ZIP архив'}\n"
        f"🆔 ID: {graph_id}"
    )

    await cb.message.answer(status_message)
    await cb.answer()


# --------------------
# API ACTION
# --------------------

async def perform_api_action(graph_id):
    """Вызывает API и отправляет данные пользователю."""
    graph = get_graph_by_id(graph_id)

    if not graph:
        logger.error(f"Граф {graph_id} не найден для выполнения API действия")
        return False

    if not graph.get("is_active", True):
        logger.info(f"Граф {graph_id} остановлен, пропускаем выполнение")
        return False

    logger.info(f"Выполняю граф {graph_id} ({graph.get('name')}) методом {graph.get('method')}")

    try:
        async with aiohttp.ClientSession() as session:
            if graph["method"] == "web":
                logger.info(f"Отправка запроса к Web API для графа {graph_id}")
                async with session.post(API_URL + "/web", json=graph["config"]) as resp:
                    if resp.status == 200:
                        j = await resp.json()
                        link = j.get("link")

                        update_graph(graph_id, last_run=datetime.now())
                        logger.info(f"Web API успешно ответил для графа {graph_id}, ссылка: {link}")

                        await bot.send_message(
                            graph["chat_id"],
                            f"📊 Ссылка на граф '{graph.get('name')}' получена!\n"
                            f"Ваш граф:\n{link}"
                        )
                        logger.info(f"Web-результат отправлен пользователю {graph['chat_id']} для графа {graph_id}")
                        return True
                    else:
                        logger.error(f"API error для графа {graph_id}: статус {resp.status}")
                        return False

            else:  # zip
                logger.info(f"Отправка запроса к ZIP API для графа {graph_id}")
                async with session.post(
                        API_URL + "/cli",
                        headers={"Content-Type": "application/json"},
                        json=graph["config"]
                ) as resp:
                    if resp.status == 200:
                        file_bytes = await resp.read()

                        update_graph(graph_id, last_run=datetime.now())
                        logger.info(
                            f"ZIP API успешно ответил для графа {graph_id}, размер архива: {len(file_bytes)} байт")

                        input_file = BufferedInputFile(
                            file=file_bytes,
                            filename=f"archive_{graph_id[:8]}.zip"
                        )

                        await bot.send_document(
                            chat_id=graph["chat_id"],
                            document=input_file,
                            caption=f"📦 ZIP архив от графа '{graph.get('name')}'"
                        )

                        logger.info(f"ZIP архив отправлен пользователю {graph['chat_id']} для графа {graph_id}")
                        return True
                    else:
                        logger.error(f"API error для графа {graph_id}: статус {resp.status}")
                        return False

    except Exception as e:
        logger.error(f"Ошибка при выполнении графа {graph_id}: {e}")

        try:
            await bot.send_message(
                graph["chat_id"],
                f"❌ Ошибка при выполнении графа '{graph.get('name')}':\n{str(e)[:200]}"
            )
            logger.info(f"Сообщение об ошибке отправлено пользователю {graph['chat_id']}")
        except Exception as send_error:
            logger.error(f"Не удалось отправить сообщение об ошибке: {send_error}")

        return False


# --------------------
# CRON CHECKER
# --------------------

async def cron_worker():
    """Фоновая задача для проверки cron-расписаний"""
    logger.info("Запущен cron worker")
    while True:
        try:
            graphs = load_graphs()
            now = datetime.now()
            logger.debug(f"Cron worker проверяет {len(graphs)} графов в {now}")

            for graph in graphs:
                if not graph.get("is_active", True) or not graph.get("cron"):
                    continue

                cron = graph["cron"]
                logger.debug(f"Проверка графа {graph['graph_id']} с cron {cron}")

                if not graph.get("next_run"):
                    logger.debug(f"Устанавливаем next_run для графа {graph['graph_id']}")
                    itr = croniter(cron, graph.get("last_run") or now)
                    next_run = itr.get_next(datetime)
                    update_graph(graph["graph_id"], next_run=next_run)
                    continue

                if graph["next_run"] <= now:
                    logger.info(f"Запускаю граф {graph['graph_id']} по cron {cron} в {now}")

                    update_graph(graph["graph_id"], last_run=now)
                    itr = croniter(cron, now)
                    next_run = itr.get_next(datetime)
                    update_graph(graph["graph_id"], next_run=next_run)

                    asyncio.create_task(perform_api_action(graph["graph_id"]))

            await asyncio.sleep(20)

        except Exception as e:
            logger.error(f"Ошибка в cron worker: {e}")
            await asyncio.sleep(60)


# --------------------
# RUN
# --------------------

async def main():
    logger.info("🤖 Запуск бота...")

    cron_task = asyncio.create_task(cron_worker())
    logger.info("Cron worker запущен")

    logger.info("Доступные команды:")
    logger.info("/start - начать работу")
    logger.info("/graphs - управление графами")
    logger.info("/new - создать новый граф")
    logger.info("/help - справка")

    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        # Отменяем cron task при завершении
        cron_task.cancel()
        try:
            await cron_task
        except asyncio.CancelledError:
            logger.info("Cron worker остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")