import asyncio
import logging
from datetime import datetime, UTC
from pathlib import Path

import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from src.services.combine_service import combine_converted_files
from src.services.converters_service import convert_csv_files_s7
from src.services.converters_service import convert_csv_files_utair
from src.services.currency_service import get_today_currency_rates
from src.services.s3_service import download_today_files
from src.services.s3_service import upload_converted_files
from src.services.telegram_service import send_all_combined_files
from src.services.telegram_service import send_all_converted_files


parent_dir = Path(__file__).parent.parent.parent
cfg_path = parent_dir / 'cfg' / 'config.yaml'

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S %Z",
)

logger = logging.getLogger(__name__)
logging.Formatter.converter = lambda *args: datetime.now(UTC).timetuple()


async def process_task(task_config):
    now = datetime.now(UTC)
    match task_config['type']:
        case "file_conversion":
            logger.info(f"task {task_config['name']} has began")
            for prefix in config['config']['S3_FETCH_PREFIXES']:
                download_today_files(prefix, task_config)
            currency_type = task_config.get("currency_type")
            convert_type = task_config.get("convert_type")
            if convert_type in CONVERTERS:
                CONVERTERS[convert_type](currency_type)
            combine_converted_files()
            upload_converted_files(task_config['name'] + "/")
            for dest in config['config']['SEND_TO']:
                if dest.startswith('telegram-user-id'):
                    chat_id = dest.split('-')[-1]
                    await send_all_converted_files(chat_id)
                    await send_all_combined_files(chat_id)
            logger.info(f"task {task_config['name']} ended")
        case "currency_fetching":
            logger.info(f"task {task_config['name']} began")
            await get_today_currency_rates(task_config['currency_type'])
            logger.info(f"task {task_config['name']} end")
        case _:
            logger.error("unknown task type from config")
            raise ValueError("unknown task type from config")

# Словарь конвертеров
CONVERTERS = {
    "utair": convert_csv_files_utair,
    "s7": convert_csv_files_s7,
    "test": convert_csv_files_utair,
}

# Чтение конфига
with open(cfg_path, 'r') as file:
    config = yaml.safe_load(file)

# Создаем планировщик
scheduler = AsyncIOScheduler(timezone="UTC")


# Обработка задач
async def schedule_tasks():
    for task in config['scheduler']['tasks']:
        if not task.get('enabled', False):  # Пропускаем отключенные задачи
            continue
        # Для каждого времени в trigger_times добавляем задачу
        for trigger_time in task['trigger_times']:
            # default = начать сейчас же
            if trigger_time == "default":
                asyncio.create_task(process_task(task))
            else:
                time_str = trigger_time.split('-utc')[0]  # Извлекаем "HH:MM"
                hour, minute = map(int, time_str.split(':'))
                # Добавляем задачу в планировщик
                scheduler.add_job(
                    process_task,
                    'cron',
                    hour=hour,
                    minute=minute,
                    args=[task],  # Передаем конфиг задачи в функцию
                    timezone="UTC"
                )


# Запускаем планировщик
async def start_scheduler():
    scheduler.start()
    logger.info("scheduler start")
    await schedule_tasks()
    await asyncio.Event().wait()
