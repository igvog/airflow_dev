import pandas as pd
import os

# Путь к твоей папке data
DATA_DIR = "./data"

files = [
    'olist_orders_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_sellers_dataset.csv',
    'olist_products_dataset.csv',
    'olist_geolocation_dataset.csv'
]

print("🧹 Начинаем очистку файлов от индекса Pandas...")

for file in files:
    path = os.path.join(DATA_DIR, file)
    if os.path.exists(path):
        # Читаем файл
        df = pd.read_csv(path)
        
        # ПРОВЕРКА: Если в файле есть колонка "Unnamed: 0" (это индекс), удаляем её
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
            print(f"✅ {file}: Удалена колонка-индекс")
        else:
            # Если колонки нет, возможно она просто первая без имени.
            # Проверяем: если колонок больше, чем ожидается, удаляем первую
            # (Простая эвристика: индекс часто идет первым)
            pass 
            
        # Перезаписываем без индекса
        df.to_csv(path, index=False)
        print(f"💾 {file}: Пересохранен чисто.")
    else:
        print(f"❌ {file}: Файл не найден!")

print("🎉 Готово! Теперь файлы чистые.")