import json
import logging

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


def get_currency_data(url: str) -> dict:
    try:
        response = requests.get(url)
        return xmltodict.parse(response.text)
    except requests.RequestException as e:
        logger.error(f"API request error: {e}")
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


def get_today_currency_rates_cbr():
    # Загрузка конфига
    parent_dir = Path(__file__).parent.parent.parent
    cfg_path = parent_dir / 'cfg' / 'config.yaml'
    config = load_config(cfg_path)
    # Чтение необходимых данных из конфига
    s3_url = config['config']['S3_URL']
    bucket_name = config['config']['S3_BUCKET_NAME']
    prefix = config['config']['S3_CURRENCY_PREFIX']
    url = config["config"]["CBR_URL"]
    today = datetime.now(UTC).date()
    data = get_currency_data(url)
    source_file = parent_dir / f"CBR_currency_rates_{today}.txt"
    save_to_json(data, source_file)
    client = Minio(s3_url,
                   access_key=os.getenv("S3_ACCESS"),
                   secret_key=os.getenv("S3_SECRET"),
                   secure=True)
    s3_object_name = f"{prefix}/CBR_currency_rates_{today}.txt"
    upload_to_s3(client, bucket_name, s3_object_name, str(source_file))


if __name__ == "__main__":
    get_today_currency_rates_cbr()


def get_today_currency_rates_otherBank():
    ...
