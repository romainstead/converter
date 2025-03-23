import glob
import logging

from minio import Minio, S3Error
from datetime import datetime, UTC, time
from pathlib import Path
from dotenv import load_dotenv
import yaml
import os

load_dotenv()

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


# Получаем время в формате datetime.time из наименования файла
def extract_time_from_filename(filename: str) -> time:
    # Выделяем время загрузки файла: 2025-03-05-01-00 -> ['01', '00']
    timestamp_str = Path(filename).stem.split("_")[2].split("-")[3::]
    # Возвращаем время загрузки файла в формате datetime.time
    timestamp = datetime.strptime(timestamp_str[0] + ":" + timestamp_str[1], "%H:%M").time()
    return timestamp


# Парсинг времени из конфига
def parse_time_range(time_range_str):
    # Получаем начало и конец временного промежутка без "-utc"
    start_str, end_str = time_range_str.split('-')[:2]
    # Получаем час и минуту начала временного промежутка в числовом виде
    start_hour, start_minute = map(int, start_str.split(':'))
    # Получаем час и минуту конца временного промежутка в числовом виде
    end_hour, end_minute = map(int, end_str.split(':'))
    # Возвращаем кортеж из двух datetime.time
    return time(start_hour, start_minute), time(end_hour, end_minute)


# Функция проверки, находится ли время в диапазоне
def is_time_in_range(check_time, time_range_str):
    start_time, end_time = parse_time_range(time_range_str)
    if start_time > end_time:  # Переход через полночь
        return check_time >= start_time or check_time <= end_time
    return start_time <= check_time <= end_time


# Функция проверки, валиден ли файл для задачи
def is_file_time_valid_for_task(filename, time_ranges):
    # Вытаскиваем время из названия файла
    file_time = extract_time_from_filename(filename)
    # Проверяем во всех time_range на случай попадания time_ranges
    for time_range in time_ranges:
        if is_time_in_range(file_time, time_range):
            return True
    return False


def download_today_files(prefix):
    # Получаем папку в которую необходимо сохранять неконвертированные файлы
    parent_dir = Path(__file__).parent.parent.parent
    need_convert = parent_dir / "need_convert"
    # Чтение конфига
    cfg_path = parent_dir / 'cfg' / 'config.yaml'
    config = load_config(cfg_path)
    # Проходим по таскам из конфига
    tasks = config['scheduler']['tasks']
    for task in tasks:
        # Извлекаем тайм рейнджи из конфига
        time_ranges = task['time_ranges']
        # Сегодняшний день
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        # Подключение к S3
        client = Minio(config['config']['S3_URL'],
                       access_key=os.getenv("S3_ACCESS"),
                       secret_key=os.getenv("S3_SECRET"),
                       secure=True)
        bucket_name = config['config']['S3_BUCKET_NAME']
        try:
            # Итерируем по списку объектов
            objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
            for obj in objects:
                # Получаем имя файла
                file_name = obj.object_name
                # Отсекаем префикс файла
                base_file_name = os.path.basename(file_name)
                if not base_file_name.startswith('prices'):
                    # logger.info(f"got incorrect filename: {base_file_name}, skipping...")
                    continue
                # Сплитим имя файла по _
                parts = base_file_name.split("_")
                # Получаем дату загрузки
                upload_date = parts[2]
                year_month_day = "-".join(upload_date.split("-")[:3])
                # Если дата загрузки совпадает с текущей датой
                if year_month_day == today:
                    # Если время загрузки попадает в time_ranges
                    if is_file_time_valid_for_task(base_file_name, time_ranges):
                        logger.info(f"downloading file: {file_name}")
                        client.fget_object(bucket_name=bucket_name,
                                           object_name=file_name,
                                           file_path=str(need_convert) + "/" + base_file_name)
                    else:
                        logger.warning(f"got file not valid for time_range: {base_file_name}, skipping...")
        except S3Error as err:
            print(err)


def upload_converted_files(prefix: str) -> None:
    # Получаем папку из которой должны браться файлы
    parent_dir = Path(__file__).parent.parent.parent
    # Путь до конфига
    cfg_path = parent_dir / 'cfg' / 'config.yaml'
    converted_dir = parent_dir / "converted"
    combined_dir = parent_dir / "combined"
    config = load_config(cfg_path)
    # Подключение к S3
    client = Minio(config['config']['S3_URL'],
                   access_key=os.getenv("S3_ACCESS"),
                   secret_key=os.getenv("S3_SECRET"),
                   secure=True)
    bucket_name = config['config']['S3_BUCKET_NAME']
    try:
        today = datetime.now(UTC).date()
        pattern_converted = f'prices_??_{today.year}-{today.month:02d}-{today.day:02d}*.csv'
        pattern_combined = f'combined_prices_??_{today.year}-{today.month:02d}-{today.day:02d}*.csv'
        # Выгружаем конвертированные файлы
        for filename in glob.glob(str(converted_dir / pattern_converted)):
            s3_object_name = f"converted/{prefix + Path(filename).stem}.csv"
            client.fput_object(bucket_name, s3_object_name, filename)
        logger.info("successfully uploaded converted files")
        # Выгружаем комбинированные файлы по компаниям
        for filename in glob.glob(str(combined_dir / pattern_combined)):
            s3_object_name = f"combined/{prefix + Path(filename).stem}.csv"
            client.fput_object(bucket_name, s3_object_name, filename)
        logger.info("successfully uploaded combined files")
    except S3Error as err:
        logger.error(f"error uploading to S3: {err}")
        raise
