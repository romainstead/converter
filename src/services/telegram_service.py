import glob
import logging
from aiogram import Bot, Dispatcher
from aiogram.types import FSInputFile, InputMediaDocument
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S %Z",
)

logger = logging.getLogger(__name__)
logging.Formatter.converter = lambda *args: datetime.now(UTC).timetuple()

# Создаём бота
bot = Bot(token=os.getenv("BOT_TOKEN"))
dp = Dispatcher()


async def send_all_converted_files(chat_id):
    today = datetime.now(UTC)
    await bot.send_message(chat_id=chat_id, text="Отправка конвертированных файлов...", request_timeout=999999)
    pattern = f'prices_??_{today.year}-{today.month:02d}-{today.day:02d}*.csv'
    files = list(glob.glob(str(converted_dir / pattern)))
    if not files:
        logger.warning(f"files not found at {converted_dir / pattern}")
        await bot.send_message(chat_id=chat_id, text="Файлы отсутствуют.")
        return
    media_group = [InputMediaDocument(media=FSInputFile(filename)) for filename in files[:10]]
    await bot.send_media_group(chat_id=chat_id, media=media_group, request_timeout=999999)
    if len(files) > 10:
        for i in range(10, len(files), 10):
            media_group = [InputMediaDocument(media=FSInputFile(filename)) for filename in files[i:i + 10]]
            await bot.send_media_group(chat_id=chat_id, media=media_group, request_timeout=999999)
    await bot.send_message(chat_id=chat_id, text="Отправка конвертированных файлов завершена.", request_timeout=999999)
    logger.info(f"ended uploading сonverted files for user {chat_id}")


async def send_all_combined_files(chat_id):
    today = datetime.now(UTC)
    await bot.send_message(chat_id=chat_id, text="Отправка комбинированных файлов...", request_timeout=999999)
    pattern = f'combined_prices_??_{today.year}-{today.month:02d}-{today.day:02d}.csv'
    files = list(glob.glob(str(combined_dir / pattern)))
    if not files:
        logger.warning(f"files not found at {combined_dir / pattern}")
        await bot.send_message(chat_id=chat_id, text="Файлы отсутствуют.")
        return
    media_group = [InputMediaDocument(media=FSInputFile(filename)) for filename in files[:10]]
    await bot.send_media_group(chat_id=chat_id, media=media_group, request_timeout=999999)
    if len(files) > 10:
        for i in range(10, len(files), 10):
            media_group = [InputMediaDocument(media=FSInputFile(filename)) for filename in files[i:i + 10]]
            await bot.send_media_group(chat_id=chat_id, media=media_group, request_timeout=999999)
    await bot.send_message(chat_id=chat_id, text="Отправка комбинированных файлов завершена.", request_timeout=999999)
    logger.info(f"ended uploading combined files for user_id: {chat_id}")

