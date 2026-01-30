import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# PostgreSQL connection parameters
POSTGRES_CONN_ID = 'postgres_default'
DB_NAME = 'airflow'
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_HOST = 'postgres'
DB_PORT = '5432'

def fetch_todos():
    """Fetch todos from JSONPlaceholder API"""
    url = "https://jsonplaceholder.typicode.com/todos"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def create_table():
    """Create todos table if not exists"""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS todos (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        title VARCHAR(255) NOT NULL,
        completed BOOLEAN NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def load_todos_to_postgres(**kwargs):
    """Load todos data into PostgreSQL"""
    todos = kwargs['ti'].xcom_pull(task_ids='fetch_todos')

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO todos (user_id, title, completed)
    VALUES (%s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        title = EXCLUDED.title,
        completed = EXCLUDED.completed,
        created_at = CURRENT_TIMESTAMP
    """

    # Prepare data for batch insert
    data = [(todo['userId'], todo['title'], todo['completed']) for todo in todos]

    execute_batch(cursor, insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'todos_dag',
    default_args=default_args,
    description='DAG to fetch todos from JSONPlaceholder and load to PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
)

# Define tasks
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

fetch_todos_task = PythonOperator(
    task_id='fetch_todos',
    python_callable=fetch_todos,
    dag=dag,
)

load_todos_task = PythonOperator(
    task_id='load_todos_to_postgres',
    python_callable=load_todos_to_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
create_table_task >> fetch_todos_task >> load_todos_task