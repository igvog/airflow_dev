# Movies ETL Data Warehouse

Этот проект реализует ETL-процесс загрузки данных о фильмах из публичного источника (The Movies Dataset) в хранилище данных (Postgres) с последующим формированием витрины данных по звёздной схеме.

Автор: **Yernas**

---

## Архитектура проекта

- **Airflow** — управление и оркестрация ETL пайплайна  
- **Postgres** — целевая база данных (Data Warehouse)  
- **Pandas** — для обработки и очистки данных  

ETL-процесс реализован в DAG под названием:

> `movies_api_to_dw`

Данные преобразуются в **звёздную модель**:

### Модель DWH (Star Schema)

- **Факт таблица**
  - `fact_rating`

- **Измерения**
  - `dim_movies`
  - `dim_user`

---

## Этапы ETL

1️ **Extract** — загрузка данных из The Movies Dataset  
2️ **Transform** — очистка, нормализация, формирование измерений и фактов  
3️ **Load** — запись в Postgres (схема `dw`)

---

## Структура проекта

AIRFLOW_DEV/
├─ dags/
│   ├─ api_to_dw_star_schema.py
│   ├─ movies_api_to_dw.py
│
│
├─ logs
├─ docker-compose.yaml
├─ requirements.txt
└─ README.md

## Запуск проекта
1. Клонировать репозиторий
2. Запустить сервисы через Docker Compose:
   docker-compose up -d
3. Открыть Airflow UI:
   http://localhost:8081
4. Запустить DAG: `movies_dataset_to_dw`

