from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

#CSV_URL = "https://data.gov.ru/portal-back/api/v1/storage?id=398fc817-b5db-4c1b-96f6-8c59aea0ceb3"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_to_postgres_dag',
    default_args=default_args,
    description='Загрузка CSV файла в PostgreSQL',
    schedule_interval=None,
    start_date=days_ago(2)
)

task_is_api_available = HttpSensor(
    task_id="is_api_available",
    http_conn_id="http_default_1",
    endpoint="/portal-back/api/v1/storage?id=398fc817-b5db-4c1b-96f6-8c59aea0ceb3",
    response_check=lambda response: "Content-Type" in response.headers and "text/csv" in response.headers["Content-Type"],
    poke_interval=5,
    timeout=60 * 5,
    dag=dag
)

task_download_csv_file = HttpOperator(
    task_id="download_csv_file",
    method="GET",
    http_conn_id="http_default_1",
    endpoint="/portal-back/api/v1/storage?id=398fc817-b5db-4c1b-96f6-8c59aea0ceb3",
    headers={"Accept": "text/csv"},
    do_xcom_push=True,  # Используем do_xcom_push вместо старого xcom_push
    log_response=False,
    dag=dag
)

def process_and_load_csv(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='download_csv_file')
    
    df = pd.read_csv(data, encoding='cp1251', sep=',', dtype=str)
    
    # Подключаемся к PostgreSQL
#    pg_hook = PostgresHook(postgres_conn_id='postgres_default')  # Убедитесь, что коннектор настроен правильно
    
    # Имя таблицы и схема БД (можно изменить)
#    table_name = 'my_table'
#    schema = 'public'  # Или любое другое значение вашей схемы
    
    # Сохраняем DataFrame в PostgreSQL
#    pg_hook.insert_rows(table=table_name, rows=df.values.tolist(), target_fields=list(df.columns))

# Задача обработки и загрузки данных
task_process_and_load_data = PythonOperator(
    task_id="process_and_load_data",
    python_callable=process_and_load_csv,
    provide_context=True,
    dag=dag
)

# Зависимости между задачами
task_is_api_available >> task_download_csv_file >> task_process_and_load_data
