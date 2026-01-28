# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Определение параметров DAG
DAG_ID = "example_airflow_api_to_postgres"
DAG_START_DATE = datetime(2023, 1, 1)

# Конфигурация логирования
logging.basicConfig(level=logging.INFO)


def fetch_todos_from_api():
    """
    Извлекает данные из API и возвращает их в виде списка словарей.
    """
    import requests
    try:
        response = requests.get("https://jsonplaceholder.typicode.com/todos")
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        todos = response.json()
        return todos
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при получении данных из API: {e}")
        return None

def insert_todos_into_postgres(todos, **kwargs):
    """
    Вставляет данные в таблицу todos в базе данных PostgreSQL.
    """
    import psycopg2
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')  # Replace 'postgres_default' with your connection ID

    if todos is None:
        logging.error("Не удалось получить данные из API, вставка в БД не производится.")
        return

    try:
        for todo in todos:
            sql = f"""
                INSERT INTO todos (userId, title, completed)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """
            pg_hook.run(sql=sql, params=(todo['userId'], todo['title'], todo['completed']))  # Use pg_hook here
        logging.info("Данные успешно вставлены в таблицу todos.")
    except Exception as e:
        logging.error(f"Ошибка при вставке данных в PostgreSQL: {e}")


with DAG(
    dag_id=DAG_ID,
    start_date=DAG_START_DATE,
    schedule_interval=None,  # Запустить вручную
    catchup=False,
    tags=['example'],
) as dag:
    # Оператор для получения данных из API
    get_todos_task = PythonOperator(
        task_id='get_todos_from_api',
        python_callable=fetch_todos_from_api,
        dag=dag,
    )

    # Оператор для вставки данных в PostgreSQL
    insert_todos_task = PythonOperator(
        task_id='insert_todos_into_postgres',
        python_callable=insert_todos_into_postgres,
        provide_context=True,
        dag=dag,
    )

    # Установление зависимости между задачами
    get_todos_task >> insert_todos_task