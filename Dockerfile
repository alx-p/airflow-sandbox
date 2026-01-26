FROM apache/airflow:2.5.1-python3.10

USER airflow

RUN pip install requests psycopg2-binary
