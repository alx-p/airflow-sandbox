from datetime import timedelta

# Импортируем необходимые модули
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Определение аргументов по умолчанию для DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Создаем экземпляр DAG
dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='Простой DAG для демонстрации',
    schedule=timedelta(days=1)
)

# Определяем первую задачу — вывод приветствия
task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

# Вторая задача — простая команда ls
task2 = BashOperator(
    task_id='list_files',
    depends_on_past=False,
    bash_command='ls -ltr /tmp/',
    retries=3,
    dag=dag
)

# Устанавливаем зависимости между задачами
task1 >> task2