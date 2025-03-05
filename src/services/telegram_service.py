import glob
import logging
from aiogram import Bot, Dispatcher
from aiogram.types import FSInputFile
from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime, UTC

# Загружаем переменные окружения
load_dotenv()

# Назначаем папку, откуда будем брать конвертированные файлы
parent_dir = Path(__file__).parent.parent.parent
converted_dir = parent_dir / 'converted'
combined_dir = parent_dir / 'combined'

# Логирование
logging.basicConfig(level=logging.INFO)

# Создаём бота
bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()


async def send_all_converted_files(chat_id):
    today = datetime.now(UTC)
    await bot.send_message(chat_id=chat_id, text="Отправка конвертированных файлов...", request_timeout=999999)
    pattern = f'prices_??_{today.year}-{today.month:02d}-{today.day:02d}*.csv'
    for filename in glob.glob(str(converted_dir / pattern)):
        document = FSInputFile(filename)
        await bot.send_document(chat_id=chat_id, document=document, request_timeout=999999)
    await bot.send_message(chat_id=chat_id, text="Отправка конвертированных файлов завершена.", request_timeout=999999)


async def send_all_combined_files(chat_id):
    today = datetime.now(UTC)
    await bot.send_message(chat_id=chat_id, text="Отправка комбинированных файлов...", request_timeout=999999)
    pattern = f'combined_prices_{today.year}-{today.month:02d}-{today.day:02d}*.csv'
    for filename in glob.glob(str(combined_dir / pattern)):
        document = FSInputFile(filename)
        await bot.send_document(chat_id=chat_id, document=document, request_timeout=999999)
    await bot.send_message(chat_id=chat_id, text="Отправка комбинированных файлов завершена.", request_timeout=999999)
