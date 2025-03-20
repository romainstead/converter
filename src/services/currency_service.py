import json
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


def get_today_currency_rates_cbr():
    # Загрузка конфига
    parent_dir = Path(__file__).parent.parent.parent
    cfg_path = parent_dir / 'cfg' / 'config.yaml'
    with open(cfg_path, 'r') as file:
        config = yaml.safe_load(file)
    # Адрес по которому будем обращаться к API
    url = f'{config["config"]["CBR_URL"]}'
    today = datetime.now(UTC).date()
    # Делаем запрос по URL
    response = requests.get(url)
    # Получаем ответ
    data = xmltodict.parse(response.text)
    # Удаление названий валют, потому что они на кириллице
    currency_data = data['ValCurs']['Valute']
    for currency in currency_data:
        del currency['Name']
    # Переводим словарь в JSON
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    source_file = f"{parent_dir}\\CBR_currency_rates_{today}.txt"
    with open(source_file, "w", encoding='utf-8') as file:
        file.write(str(json_data))
    # Подключение к S3
    client = Minio(config['config']['S3_URL'],
                   access_key=os.getenv("S3_ACCESS"),
                   secret_key=os.getenv("S3_SECRET"),
                   secure=True)
    bucket_name = config['config']['S3_BUCKET_NAME']
    # Загружаем файл в S3
    try:
        prefix = config['config']['S3_CURRENCY_PREFIX']
        client.fput_object(bucket_name, f"{prefix}/CBR_currency_rates_{today}.txt", source_file)
    except S3Error as err:
        print(err)


def get_today_currency_rates_otherBank():
    ...