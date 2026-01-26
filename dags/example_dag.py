from datetime import timedelta
import requests
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator


# Функция для получения данных из API
def fetch_data_from_api():
    response = requests.get('https://jsonplaceholder.typicode.com/todos')
    return response.json()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='fetch_and_load_data',
    default_args=default_args,
    description='Fetch data from an API and load it into a database',
    schedule_interval=None, # Измените на нужное расписание, например '@daily' для ежедневного запуска
    start_date=days_ago(2)
)

create_table_task = PostgresOperator(
    task_id="create_table",
    sql="""
    CREATE TABLE IF NOT EXISTS todos (
        id SERIAL PRIMARY KEY,
        userId INT,
        title VARCHAR(255),
        completed BOOLEAN
    );
    """,
    postgres_conn_id="postgres_default",
    dag=dag
)

fetch_data_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data_from_api,
    dag=dag
)

load_data_to_db_task = PostgresOperator(
    task_id="load_data_into_db",
    sql="""
    INSERT INTO todos (id, userId, title, completed)
    VALUES (%s, %s, %s, %s);
    """,
    parameters=lambda context: [(item['id'], item['userId'], item['title'], item['completed']) for item in context['task_instance'].xcom_pull(task_ids='fetch_data')],
    postgres_conn_id="postgres_default",
    dag=dag
)

create_table_task >> fetch_data_task >> load_data_to_db_task