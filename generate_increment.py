import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
import os

# --- КОНФИГУРАЦИЯ ---
DATA_DIR = "./data"
NUM_NEW_ORDERS = 50  # Сколько заказов генерируем за раз
# ---------------------

def load_ids(filename, col_name):
    """Безопасно загружает уникальные ID из файла."""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Файл {filename} не найден! Запустите сначала prepare_data.py")
    # Читаем только нужную колонку для скорости
    df = pd.read_csv(path, usecols=[col_name])
    return df[col_name].unique()

def generate_synthetic_data():
    print(f"🚀 Начинаем генерацию {NUM_NEW_ORDERS} синтетических заказов...")
    
    # 1. ЗАГРУЗКА СУЩЕСТВУЮЩИХ ID (ДЛЯ FK)
    # Нам нужны реальные ID, чтобы база данных приняла наши новые строки
    try:
        customer_ids = load_ids('olist_customers_dataset.csv', 'customer_id')
        product_ids = load_ids('olist_products_dataset.csv', 'product_id')
        seller_ids = load_ids('olist_sellers_dataset.csv', 'seller_id')
    except Exception as e:
        print(e)
        return

    # 2. ГЕНЕРАЦИЯ ДАННЫХ
    new_orders = []
    new_items = []
    new_payments = []
    
    current_time = datetime.now()
    # Имитируем, что заказы падали в течение последних 24 часов
    
    for _ in range(NUM_NEW_ORDERS):
        # Генерируем ключи
        order_uuid = str(uuid.uuid4())
        
        # Случайное время заказа (вчера-сегодня)
        minutes_offset = np.random.randint(0, 1440)
        order_date = current_time - timedelta(minutes=minutes_offset)
        order_date_str = order_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # Логистика дат
        approved_at = (order_date + timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
        pickup_at = (order_date + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        delivered_at = (order_date + timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
        estimated_at = (order_date + timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S')

        # Финансы
        price = round(np.random.lognormal(4.5, 0.5), 2) # Реалистичное распределение цен
        freight = round(np.random.uniform(10, 50), 2)   # Доставка
        total_value = price + freight

        # A) TAB: ORDERS
        new_orders.append({
            'order_id': order_uuid,
            'customer_id': np.random.choice(customer_ids), # Берем существующего клиента
            'order_status': 'delivered',
            'order_purchase_timestamp': order_date_str,
            'order_approved_at': approved_at,
            'order_delivered_carrier_date': pickup_at,
            'order_delivered_customer_date': delivered_at,
            'order_estimated_delivery_date': estimated_at
        })

        # B) TAB: ORDER_ITEMS (Критично для GMV!)
        new_items.append({
            'order_id': order_uuid,
            'order_item_id': 1, # Пока по 1 товару в заказе для простоты
            'product_id': np.random.choice(product_ids),
            'seller_id': np.random.choice(seller_ids),
            'shipping_limit_date': pickup_at,
            'price': price,
            'freight_value': freight
        })

        # C) TAB: ORDER_PAYMENTS
        new_payments.append({
            'order_id': order_uuid,
            'payment_sequential': 1,
            'payment_type': np.random.choice(['credit_card', 'boleto', 'pix']),
            'payment_installments': 1,
            'payment_value': total_value # Сумма сходится с Items
        })

    # 3. СОХРАНЕНИЕ (APPEND)
    # Важно: columns должны идти в том же порядке, что и в CSV.
    # Pandas to_csv(mode='a') просто дописывает, он не проверяет заголовки, если header=False.
    # Поэтому мы явно упорядочиваем колонки.

    # Списки колонок (порядок из оригинальных файлов Olist)
    cols_orders = ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 
                   'order_approved_at', 'order_delivered_carrier_date', 
                   'order_delivered_customer_date', 'order_estimated_delivery_date']
    
    cols_items = ['order_id', 'order_item_id', 'product_id', 'seller_id', 
                  'shipping_limit_date', 'price', 'freight_value']
    
    cols_payments = ['order_id', 'payment_sequential', 'payment_type', 
                     'payment_installments', 'payment_value']

    # Запись
    pd.DataFrame(new_orders)[cols_orders].to_csv(
        os.path.join(DATA_DIR, 'olist_orders_dataset.csv'), mode='a', header=False, index=False
    )
    
    pd.DataFrame(new_items)[cols_items].to_csv(
        os.path.join(DATA_DIR, 'olist_order_items_dataset.csv'), mode='a', header=False, index=False
    )
    
    pd.DataFrame(new_payments)[cols_payments].to_csv(
        os.path.join(DATA_DIR, 'olist_order_payments_dataset.csv'), mode='a', header=False, index=False
    )

    print(f"✅ Успешно сгенерировано {NUM_NEW_ORDERS} полных транзакций (Orders + Items + Payments).")

if __name__ == "__main__":
    generate_synthetic_data()