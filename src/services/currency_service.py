import requests
import os
import dotenv
from pathlib import Path
import yaml
from datetime import datetime, UTC
from minio import Minio, S3Error
import yaml

# Загружаем переменные окружения
dotenv.load_dotenv()
API_KEY = os.getenv("EXCHANGE_RATES_API_KEY")


def get_today_rates():
    # Загрузка конфига
    parent_dir = Path(__file__).parent.parent.parent
    cfg_path = parent_dir / 'cfg' / 'config.yaml'
    with open(cfg_path, 'r') as file:
        config = yaml.safe_load(file)

    # Получаем нужную валюту из конфига
    currency = config['scheduler']['currency']

    # Адрес по которому будем обращаться к API
    url = f'{config["config"]["CURRENCY_URL"]}/{API_KEY}/latest/{currency}'
    today = datetime.now(UTC).date()
    # Делаем запрос по URL
    response = requests.get(url)
    # Получаем ответ
    data = response.json()
    # Формируем файл с курсом валют
    source_file = f"{parent_dir}\\currency_rates_{currency}_{today}"
    with open(source_file, "w") as cur:
        cur.write(str(data['conversion_rates']))
    # Подключение к S3
    client = Minio(config['config']['S3_URL'],
                   access_key=os.getenv("S3_ACCESS"),
                   secret_key=os.getenv("S3_SECRET"),
                   secure=True)
    bucket_name = config['config']['S3_BUCKET_NAME']
    # Загружаем файл в S3
    try:
        prefix = config['config']['S3_CURRENCY_PREFIX']
        client.fput_object(bucket_name, f"{prefix}/currency_rates_{currency}_{today}.txt", source_file)
    except S3Error as err:
        print(err)


if __name__ == "__main__":
    get_today_rates()
