from api_ops import fetch_api_data
from telegram_ops import send_telegram_message

OPERATIONS = {
    "fetch_api_data": fetch_api_data,
    "send_telegram_message": send_telegram_message
}