import pandas as pd
import os
import argparse
import logging

# Настройка логирования для скрипта
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_daily_data(target_date):
    """
    Читает большой исторический CSV и извлекает данные за указанную дату (target_date).
    Эмулирует ответ API для ежедневной пакетной загрузки.
    """
    base_path = "/opt/airflow/data"
    source_file = os.path.join(base_path, "retail.csv")
    output_file = os.path.join(base_path, f"sales_{target_date}.csv")
    
    logging.info(f"Начало извлечения данных за дату: {target_date}")    
    
    
    if not os.path.exists(source_file):
        logging.error(f"Исходный файл {source_file} не найден.")
        return

    try:
        # Используем кодировку ISO-8859-1 для корректной обработки спецсимволов
        df = pd.read_csv(source_file, encoding='ISO-8859-1')
        
        # 3. Определяем колонку с датой
        date_col = 'InvoiceDate'
        if date_col not in df.columns:
            # Пробуем альтернативы, если файл другой
            possible_cols = ['Order Date', 'Date', 'invoice_date']
            for col in possible_cols:
                if col in df.columns:
                    date_col = col
                    break
        

        # Преобразование в datetime и фильтрация по целевой дате
        df[date_col] = pd.to_datetime(df[date_col])
        target_dt = pd.to_datetime(target_date).date()
        daily_data = df[df[date_col].dt.date == target_dt]
        
        if daily_data.empty:
            logging.warning(f"Транзакции за {target_date} не найдены")
        else:
            daily_data.to_csv(output_file, index=False)
            logging.info(f"Успешно извлечено {len(daily_data)} строк в файл {output_file}")

    except Exception as e:
        logging.error(f"Ошибка обработки данных: {e}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    extract_daily_data(args.date)