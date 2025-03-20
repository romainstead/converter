import logging

import pandas as pd
import os
from glob import glob
from pathlib import Path
from datetime import datetime, UTC

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S %Z",
)

logger = logging.getLogger(__name__)
logging.Formatter.converter = lambda *args: datetime.now(UTC).timetuple()


def combine_converted_files():
    parent_dir = Path(__file__).parent.parent.parent
    directory = parent_dir / 'converted'

    output_directory = parent_dir / 'combined'

    # Поиск всех CSV файлов в директории
    all_files = glob(os.path.join(directory, "prices_*.csv"))

    # Словарь для хранения данных по компаниям
    company_data = {}

    # Проходим по всем файлам
    for file in all_files:
        # Извлекаем название компании из имени файла
        filename = os.path.basename(file)
        company = filename.split('_')[1]

        # Читаем CSV файл
        df = pd.read_csv(file)

        # Если компания еще не в словаре, создаем пустой список
        if company not in company_data:
            company_data[company] = []

        # Добавляем датафрейм в список для этой компании
        company_data[company].append(df)

    # Объединяем файлы для каждой компании и сохраняем
    for company, dfs in company_data.items():
        # Объединяем все датафреймы для этой компании
        combined_df = pd.concat(dfs, ignore_index=True)

        # Создаем имя выходного файла
        output_file = f'combined_prices_{company}_{datetime.now(UTC).strftime("%Y-%m-%d")}.csv'

        output_path = os.path.join(output_directory, output_file)

        # Сохраняем объединенный датафрейм в CSV
        combined_df.to_csv(output_path, index=False)
        logger.info(f"created file for company {company}: {Path(output_path).stem}")

# if __name__ == "__main__":
#     convert_final()
