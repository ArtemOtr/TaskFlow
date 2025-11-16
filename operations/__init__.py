from .api_ops import fetch_api_data
from .telegram_ops import send_telegram_message
from .data_ops import dict_to_string, json_to_string

OPERATIONS = {
    "fetch_api_data": fetch_api_data,
    "send_telegram_message": send_telegram_message,
    "dict_to_string": dict_to_string,
    "json_to_string": json_to_string
}