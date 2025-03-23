import json
import logging

import aiohttp
import requests
import os
import dotenv
from pathlib import Path
import xmltodict
from datetime import datetime, UTC
from minio import Minio, S3Error
import yaml

# Загружаем переменные окружения
dotenv.load_dotenv()

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S %Z",
)

logger = logging.getLogger(__name__)
logging.Formatter.converter = lambda *args: datetime.now(UTC).timetuple()


def load_config(config_path: Path) -> dict:
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
            return config
    except Exception as e:
        logger.error(f"error loading config file: {e}")


async def get_currency_data_xml(session: aiohttp.ClientSession, url: str) -> dict:
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            text = await response.text()
            return xmltodict.parse(text)
    except aiohttp.ClientError as e:
        logger.error(f"request error to {url}: {e}")
        raise


async def get_currency_data_json(session: aiohttp.ClientSession, url: str) -> dict:
    try:
        async with session.get(url) as response:
            text = await response.text()
            data = json.loads(text)
            return data
    except aiohttp.ClientError as e:
        logger.error(f"request error to {url}: {e}")
        raise


def save_to_json(data: dict, file_path: Path) -> None:
    try:
        json_data = json.dumps(data, indent=4, ensure_ascii=False)
        with open(file_path, "w", encoding='utf-8') as file:
            file.write(json_data)
    except Exception as e:
        logger.error(f"error saving to JSON: {e}")


def upload_to_s3(client: Minio, bucket_name: str, object_name: str, file_path: str) -> None:
    try:
        client.fput_object(bucket_name, object_name, file_path)
        logger.info(f"uploaded file to S3: {object_name}")
    except S3Error as e:
        logger.error(f"error uploading to S3: {e}")
        raise


async def get_today_currency_rates(currency_type: str) -> None:
    logger.info(f"currency fetching from {currency_type} begin")
    today = datetime.now(UTC).date()

    # Загрузка конфига
    parent_dir = Path(__file__).parent.parent.parent
    cfg_path = parent_dir / 'cfg' / 'config.yaml'
    config = load_config(cfg_path)

    # Чтение необходимых данных из конфига
    s3_url = config['config']['S3_URL']
    bucket_name = config['config']['S3_BUCKET_NAME']
    prefix = config['config']['S3_CURRENCY_PREFIX']

    # Запросы в зависимости от банка
    async with aiohttp.ClientSession() as session:
        match currency_type:
            case "CBR":
                url = config['config']['CBR_URL']
                data = await get_currency_data_xml(session, url)
            case "NBRB":
                url = config['config']['NBRB_URL']
                data = await get_currency_data_json(session, url)
            case _:
                logger.error("unsupported currency type")
                raise ValueError("unsupported currency type")

    # Создание файла и запись данных в него
    source_file = parent_dir / f"{currency_type}_currency_rates_{today}.txt"
    save_to_json(data, source_file)

    # Отгрузка в С3
    client = Minio(s3_url,
                   access_key=os.getenv("S3_ACCESS"),
                   secret_key=os.getenv("S3_SECRET"),
                   secure=True)
    s3_object_name = f"{prefix}/{currency_type}_currency_rates_{today}.txt"
    upload_to_s3(client, bucket_name, s3_object_name, str(source_file))
    os.remove(source_file)
    logger.info(f"success currency fetching from {currency_type}")

# if __name__ == "__main__":
#     get_today_currency_rates("NBRB")
