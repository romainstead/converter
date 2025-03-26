from datetime import datetime, UTC
import logging
import os
import pandas as pd
from pathlib import Path
from src.services.s3_service import download_currency_from_s3, load_config

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S %Z",
)

logger = logging.getLogger(__name__)
logging.Formatter.converter = lambda *args: datetime.now(UTC).timetuple()


def search_for_currency(currency_list: list, key: str, need_currency: str):
    return [element for element in currency_list if element[f'{key}'] == need_currency]


def convert_csv_files_utair(currency_type: str):
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    # Папки для работы
    parent_dir = Path(__file__).parent.parent.parent
    input_folder = parent_dir / 'need_convert'
    output_folder = parent_dir / 'converted'
    cfg_path = parent_dir / 'cfg' / "config.yaml"

    # Загружаем конфиг
    config = load_config(cfg_path)
    # Читаем из конфига нужную для конвертации валюту
    need_currency = config['scheduler']['currency']
    # Загружаем актуальный курс
    # TODO: ПРОВЕРИТЬ, НЕТ ЛИ ХАРДКОДА В download_currency_from_s3()
    currency_data = download_currency_from_s3(config['config']['S3_CURRENCY_PREFIX'], currency_type)
    exchange_rate = 1
    # Если курс российского рубля
    if currency_type == 'CBR':
        # Убираем ненужные данные
        clean_currency_data = currency_data['ValCurs']['Valute']
        # Ищем данные по нужной валюте
        need_cur = search_for_currency(clean_currency_data, config['config']['CBR_CURRENCY_SEARCH_KEY'], need_currency)
        # Если такая валюта найдена, то всё ок
        if any(need_cur):
            # ЦБ РФ пишет десятичные значения через запятую
            # Заменим её на точку и переведём во float
            exchange_rate_str = need_cur[0][config['config']['CBR_MAIN_KEY']].replace(',', '.')
            exchange_rate = float(exchange_rate_str)
        # Если не найдена, то exchange_rate = 1
        else:
            exchange_rate = 1
            logger.warning("needed currency not found")
    elif currency_type == 'NBRB':
        need_cur = search_for_currency(currency_data, config['config']['NBRB_CURRENCY_SEARCH_KEY'], need_currency)
        if any(need_cur):
            exchange_rate = need_cur[0]['Cur_OfficialRate'] / need_cur[0]['Cur_Scale']
        else:
            exchange_rate = 1
            logger.warning("needed currency not found")

    # Создаем выходную папку, если ее нет
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Выходные колонки согласно ТЗ
    output_columns = [
        'cxr', 'orig atp', 'dest apt', 'first fcc', 'last fcc', 'first flt#', 'first flt date',
        'last flt#', 'last flt date', 'season', 'comp', 'amt', 'inclusive', 'tax',
        'cur', 'fty', 'advpur', 'minstay', 'maxstay', 'first ticket', 'last ticket',
        'first travel', 'last travel', 'penalty', 'shapshot', 'type', 'pos',
        'outbound_travel_stop_over', 'homebound_travel_stop_over', 'out_booking_class',
        'home_booking_class', 'source'
    ]

    # Проходим по всем файлам в папке need_convert
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            # Читаем входной CSV файл
            input_path = os.path.join(input_folder, filename)
            df = pd.read_csv(input_path)

            # Извлекаем код перевозчика из имени файла
            cxr = filename.split('_')[1]

            # Создаем новый DataFrame с нужными колонками
            new_df = pd.DataFrame(columns=output_columns)
            new_df['cxr'] = [cxr] * len(df) if not df.empty else [cxr]

            # Заполняем поля согласно ТЗ
            new_df['orig atp'] = df.get('origin', '')
            new_df['dest apt'] = df.get('destination', '')
            new_df['first fcc'] = ''
            new_df['first flt#'] = df.get('outbound_flight_no', '')

            # Форматирование first flt date в DD.MM.YYYY HH:MM
            first_flt_date = df.get('outbound_departure_date', '')
            if not first_flt_date.empty:
                new_df['first flt date'] = pd.to_datetime(first_flt_date, format='mixed', dayfirst=True).dt.strftime(
                    '%d.%m.%Y %H:%M')
            else:
                new_df['first flt date'] = ''

            # Добавляем данные в comp, если есть колонка service_class
            new_df['comp'] = df.get('service_class', '')

            # Учитываем оба варианта price_exc/price_ex и добавляем .0
            if 'price_exc' in df.columns:
                # Переводим столбец в числовой тип
                numeric_amt = pd.to_numeric(df['price_exc'], errors='coerce')
                # Делим на курс обмена
                numeric_amt = numeric_amt / exchange_rate
                # Переводим в строки
                new_df['amt'] = numeric_amt.map(lambda x: f"{x:.1f}" if pd.notnull(x) else '')
                new_df['inclusive'] = new_df['amt']

            elif 'price_ex' in df.columns:
                # Переводим столбец в числовой тип
                numeric_amt = pd.to_numeric(df['price_ex'], errors='coerce')
                # Делим на курс обмена
                numeric_amt = numeric_amt / exchange_rate
                # Переводим в строку
                new_df['amt'] = numeric_amt.map(lambda x: f"{x:.1f}" if pd.notnull(x) else '')
                new_df['inclusive'] = new_df['amt']
            else:
                new_df['amt'] = ''
                new_df['inclusive'] = ''
            # Переводим в числовой формат
            numeric_tax = pd.to_numeric(df.get('tax', ''), errors='coerce')
            # Делим на курс обмена
            numeric_tax = numeric_tax / exchange_rate
            # Переводим в строку
            new_df['tax'] = numeric_tax.map(lambda x: f"{x:.1f}" if pd.notnull(x) else '')
            # Заменяем название курса на тот, в который конвертировали
            new_df['cur'] = need_currency
            new_df['fty'] = 'OW'

            # Форматирование shapshot в DD.MM.YYYY HH:MM
            shapshot_raw = df['observation_date'] + ' ' + df['observation_time']
            new_df['shapshot'] = pd.to_datetime(shapshot_raw, format='mixed', dayfirst=True).dt.strftime(
                '%d.%m.%Y %H:%M')

            new_df['out_booking_class'] = df.get('booking_class', '')
            new_df['source'] = df.get('source', '')

            # Оставляем пустыми поля, не указанные в явном маппинге ТЗ
            new_df['last fcc'] = ''
            new_df['last flt#'] = ''
            new_df['last flt date'] = ''
            new_df['season'] = ''
            new_df['advpur'] = ''
            new_df['minstay'] = ''
            new_df['maxstay'] = ''
            new_df['first ticket'] = ''
            new_df['last ticket'] = ''
            new_df['first travel'] = ''
            new_df['last travel'] = ''
            new_df['penalty'] = ''
            new_df['type'] = ''
            new_df['pos'] = ''
            new_df['outbound_travel_stop_over'] = df.get('transfer_iata', '')
            new_df['homebound_travel_stop_over'] = ''
            new_df['home_booking_class'] = ''

            # # Проверяем cxr перед сохранением
            # logger.info(f"first 5 new_df['cxr'] перед сохранением: {new_df['cxr'].head().tolist()}")

            # Сохраняем результат в выходной файл
            output_path = os.path.join(output_folder, filename)
            new_df.to_csv(output_path, index=False, encoding='utf-8')

            logger.info(f"file is converted: {Path(filename).stem}")

    logger.info("file conversion is end")


def convert_csv_files_s7():
    print("foo")
