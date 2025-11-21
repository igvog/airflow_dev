from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle

# Путь к PDF
pdf_path = "ETL_Report.pdf"
doc = SimpleDocTemplate(pdf_path, pagesize=A4)
styles = getSampleStyleSheet()
story = []

# ===== 1. Постановка задачи =====
story.append(Paragraph("1. Постановка задачи", styles['Heading1']))
story.append(Paragraph(
    "Цель: Создать ETL pipeline для двух источников данных: Online Retail II и OECD Health. "
    "Необходимо загружать данные в staging таблицы, нормализовать, создать DWH (dim + fact) и объединить данные для аналитики.", 
    styles['Normal']
))
story.append(Spacer(1, 12))

# ===== 2. Архитектура =====
story.append(Paragraph("2. Архитектура и подход к решению", styles['Heading1']))
story.append(Paragraph(
    "Архитектура ETL: CSV/API → Staging → DWH (dim + fact) → Merge/Analysis.\n"
    "Подход: Python + pandas, Airflow DAG с PythonOperator и PostgresOperator, нормализация колонок, управление NULL/NaN, foreign keys для связей fact и dim.", 
    styles['Normal']
))
story.append(Spacer(1, 12))

# Место для диаграммы архитектуры
story.append(Paragraph("Диаграмма архитектуры ETL (вставьте скриншот):", styles['Normal']))
story.append(Spacer(1, 12))
story.append(Image("placeholder_architecture.png", width=400, height=200))  # вставьте свой скриншот
story.append(Spacer(1, 12))

# ===== 3. Пошаговое описание реализации =====
story.append(Paragraph("3. Пошаговое описание реализации", styles['Heading1']))
steps = [
    "1. Загрузка данных в staging таблицы через PythonOperator.",
    "2. Нормализация названий колонок и обработка пустых значений.",
    "3. Создание DWH: dim_country, dim_products, dim_customers, dim_date, dim_country_oecd, dim_indicator.",
    "4. Загрузка данных в DWH с соблюдением foreign key и обработкой дубликатов.",
    "5. Объединение таблиц fact_sales и fact_oecd_health для аналитики."
]
for s in steps:
    story.append(Paragraph(s, styles['Normal']))
    story.append(Spacer(1, 6))

story.append(Spacer(1, 12))
story.append(Paragraph("Примеры кода и SQL:", styles['Normal']))
story.append(Paragraph("INSERT INTO dim_country ...", styles['Code']))
story.append(Spacer(1, 12))

# ===== 4. Возникшие трудности =====
story.append(Paragraph("4. Возникшие трудности и их решение", styles['Heading1']))
difficulties = [
    "Ошибка с foreign key (int vs varchar) → исправлено через унификацию типов.",
    "Пустые колонки flag_codes и flags → заменены на NULL в staging.",
    "Ошибки при merge → решено с помощью DISTINCT и JOIN по уникальным колонкам."
]
for d in difficulties:
    story.append(Paragraph(d, styles['Normal']))
    story.append(Spacer(1, 6))
story.append(Spacer(1, 12))

# ===== 5. Итоговые результаты =====
story.append(Paragraph("5. Итоговые результаты", styles['Heading1']))
story.append(Paragraph(
    "Успешный запуск DAG, созданные dimension и fact таблицы, результаты merge двух источников (сводные данные по странам).",
    styles['Normal']
))
story.append(Spacer(1, 12))

# Скриншоты Airflow и таблиц
story.append(Paragraph("Скриншоты Airflow UI:", styles['Normal']))
story.append(Image("placeholder_airflow_ui.png", width=400, height=200))
story.append(Spacer(1, 12))

story.append(Paragraph("Скриншоты таблиц и данных:", styles['Normal']))
story.append(Image("placeholder_table.png", width=400, height=200))
story.append(Spacer(1, 12))

# ===== Сборка PDF =====
doc.build(story)
print(f"PDF отчет создан: {pdf_path}")
