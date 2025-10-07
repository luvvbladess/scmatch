#Токены

import logging
import os

# Конфиг из переменных окружения
BOT_TOKEN = "xxx"
OPENAI_API_KEY = "xxx"

if not BOT_TOKEN or BOT_TOKEN == "TELEGRAM_BOT_TOKEN_HERE":
    raise RuntimeError("Укажите реальный BOT_TOKEN в переменной BOT_TOKEN в коде.")
if not OPENAI_API_KEY or OPENAI_API_KEY == "OPENAI_API_KEY_HERE":
    raise RuntimeError("Укажите реальный OPENAI_API_KEY в переменной OPENAI_API_KEY в коде.")

DB_PATH = "dating_bot.sqlite3"
AGE_DELTA = 2  # возрастной допуск при поиске (±2 года)
CANDIDATES_LIMIT = 30  # размер пула кандидатов для подбора
EMBED_MODEL = "text-embedding-3-small"
CHAT_MODEL = "gpt-4o-mini"
OPENAI_TIMEOUT = 30.0  # секунды

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dating-bot")



