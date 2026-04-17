from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
from sqlalchemy import create_engine

MINIO_ENDPOINT = "minio"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "raw-data"

POSTGRES_HOST = "postgres"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
POSTGRES_DB = "airflow"
POSTGRES_PORT = "5432"

def upload_to_minio(**kwargs):
    df = pd.read_csv("/opt/airflow/dags/sample_data.csv")
    file_name = "raw_data.csv"

    # Сохраняем в временный файл
    temp_file = f"/tmp/{file_name}"
    df.to_csv(temp_file, index=False)

    # Загружаем в MinIO
    client = boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}:9000",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )

    # Создаем бакет, если его нет
    try:
        client.create_bucket(Bucket=MINIO_BUCKET)
    except client.exceptions.BucketAlreadyExists:
        pass

    # Загружаем файл
    client.upload_file(temp_file, MINIO_BUCKET, file_name)
    print(f"Файл {file_name} успешно загружен в MinIO")

def clean_and_load_to_postgres(**kwargs):
    
    client = boto3.client(
        's3',
        endpoint_url=f"http://{MINIO_ENDPOINT}:9000",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )

    
    file_name = "raw_data.csv"
    temp_file = f"/tmp/{file_name}"
    client.download_file(MINIO_BUCKET, file_name, temp_file)

    
    df = pd.read_csv(temp_file)
    
    df = df.drop_duplicates().dropna()

    # Сохраняем очищенные данные в PostgreSQL
    engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    df.to_sql('clean_data', engine, if_exists='replace', index=False)
    print("Очищенные данные успешно загружены в PostgreSQL")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Pipeline for loading raw data to MinIO and clean data to PostgreSQL',
    schedule_interval=timedelta(days=1),
)

upload_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_and_load_to_postgres',
    python_callable=clean_and_load_to_postgres,
    dag=dag,
)

upload_task >> clean_task