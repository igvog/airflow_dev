import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# --- 1. НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ---
DB_CONFIG = {
    "host": "localhost",
    "port": "5439",
    "database": "kaspi_lab_db",
    "user": "user",
    "password": "password"
}

# --- 2. ФУНКЦИЯ ЗАГРУЗКИ ---
@st.cache_data(ttl=10) # Кэш 10 секунд для "live" эффекта
def load_data(query):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Ошибка подключения: {e}")
        return None

# --- 3. SQL ЗАПРОСЫ ---

# Бизнес-метрики (как было)
QUERY_BIZ_METRICS = """
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(gross_merchandise_value) AS total_gmv,
        AVG(gross_merchandise_value) AS avg_ticket,
        AVG(delivery_time_days) AS avg_delivery
    FROM marts.sales_items
    WHERE order_status = 'delivered';
"""

QUERY_BIZ_CHART = """
    SELECT 
        order_date::DATE as day,
        SUM(gross_merchandise_value) as daily_gmv
    FROM marts.sales_items
    WHERE order_date >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY 1 ORDER BY 1;
"""

# Data Quality Метрики (НОВОЕ!)
# Считаем NULLs и дубликаты в главной витрине
QUERY_DQ_METRICS = """
    SELECT
        COUNT(*) as total_rows,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_ids,
        SUM(CASE WHEN gross_merchandise_value < 0 THEN 1 ELSE 0 END) as negative_gmv,
        -- Проверка дублей (упрощенно: если count > distinct count)
        COUNT(*) - COUNT(DISTINCT order_id || '-' || product_id) as duplicate_rows
    FROM marts.sales_items;
"""

# --- 4. ИНТЕРФЕЙС ---
st.set_page_config(page_title="Kaspi Lab Platform", layout="wide", page_icon="🚀")

st.title("🚀 Kaspi Lab: E-Commerce Data Platform")

# Вкладки
tab1, tab2 = st.tabs(["📊 Business Analytics", "🛡️ Data Quality Monitor"])

# --- TAB 1: БИЗНЕС ---
with tab1:
    st.subheader("Real-time Business Overview")
    
    metrics_df = load_data(QUERY_BIZ_METRICS)
    chart_df = load_data(QUERY_BIZ_CHART)

    if metrics_df is not None:
        m = metrics_df.iloc[0]
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("💰 GMV", f"R$ {m['total_gmv']:,.0f}")
        kpi2.metric("📦 Orders", f"{m['total_orders']:,}")
        kpi3.metric("🏷️ AOV", f"R$ {m['avg_ticket']:.1f}")
        kpi4.metric("🚚 Avg Delivery", f"{m['avg_delivery']:.1f} d")
        
        st.divider()
        
        if chart_df is not None and not chart_df.empty:
            fig = px.bar(chart_df, x='day', y='daily_gmv', title="Daily GMV Trend (Live)")
            fig.update_traces(marker_color='#00CC96')
            st.plotly_chart(fig, use_container_width=True)

# --- TAB 2: КАЧЕСТВО ДАННЫХ (DQ) ---
with tab2:
    st.subheader("🛡️ Data Quality Health Check")
    st.caption("Мониторинг здоровья данных в слое Gold (marts.sales_items)")
    
    dq_df = load_data(QUERY_DQ_METRICS)
    
    if dq_df is not None:
        d = dq_df.iloc[0]
        
        # Рассчет процентов
        total = d['total_rows']
        null_pct = (d['null_ids'] / total) * 100
        neg_gmv_count = d['negative_gmv']
        dupes = d['duplicate_rows']
        
        # Отображение метрик DQ
        c1, c2, c3, c4 = st.columns(4)
        
        c1.metric("Total Rows", f"{total:,}", delta="Daily Increment")
        
        c2.metric("Null IDs", f"{d['null_ids']} ({null_pct:.2f}%)", 
                  delta_color="inverse", delta="Goal: 0%")
                  
        c3.metric("Negative GMV Errors", f"{neg_gmv_count}", 
                  delta_color="inverse", delta="Must be 0")
                  
        c4.metric("Duplicate Rows", f"{dupes}", 
                  delta_color="inverse", delta="Must be 0")
        
        st.divider()
        
        # Алерты
        if neg_gmv_count > 0:
            st.error(f"🚨 АНОМАЛИЯ: Найдены заказы с отрицательной суммой: {neg_gmv_count}")
        else:
            st.success("✅ Финансовых аномалий не обнаружено.")
            
        if dupes > 0:
            st.warning(f"⚠️ Внимание: Найдены потенциальные дубликаты: {dupes}")
        else:
            st.success("✅ Дубликатов нет. Уникальность ключей соблюдена.")

    # Кнопка принудительного обновления
    if st.button('🔄 Re-run DQ Checks'):
        st.rerun()