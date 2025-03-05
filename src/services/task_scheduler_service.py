import asyncio
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import yaml
from datetime import datetime, UTC
from src.services.telegram_service import send_all_converted_files
from src.services.telegram_service import send_all_combined_files
from src.services.converters_service import convert_csv_files_utair
from src.services.converters_service import convert_csv_files_s7
from src.services.combine_service import combine_converted_files
from src.services.s3_service import download_today_files
from src.services.s3_service import upload_converted_files

parent_dir = Path(__file__).parent.parent.parent
cfg_path = parent_dir / 'cfg' / 'config.yaml'


async def process_task(task_config):
    now = datetime.now(UTC)
    print(f"Задача {task_config['name']} начата в {now}")
    # Загрузка файлов из папок Игоря и Айтала
    download_today_files("rivals/")
    download_today_files("rivals_i/")
    # Получаем тип конвертера из конфига
    convert_type = task_config.get("type")
    # Если тип конвертера присутствует есть в словаре, то вызываем его
    if convert_type in CONVERTERS:
        CONVERTERS[convert_type]()
    # Комбинируем файлы по перевозчикам
    combine_converted_files()
    # Загружаем конвертированные и комбинированные файлы
    # Префикс у файлов будет по нейму из конфига
    upload_converted_files(task_config['name'] + "/")
    for dest in task_config['send_to']:
        # Выгрузка в телеграм
        if dest.startswith('telegram-user-id'):
            chat_id = dest.split('-')[-1]
            await send_all_converted_files(chat_id)
            await send_all_combined_files(chat_id)
    print(f"Задача {task_config['name']} завершена в {now}")


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
        # Для каждого времени в convert_times добавляем задачу
        for convert_time in task['convert_times']:
            # default = начать сейчас же
            if convert_time == "default":
                asyncio.create_task(process_task(task))
            else:
                time_str = convert_time.split('-utc')[0]  # Извлекаем "HH:MM"
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
    print("Планировщик запущен...")
    await schedule_tasks()
    await asyncio.Event().wait()
