FROM apache/airflow:2.10.5-python3.12

USER airflow

RUN pip install requests psycopg2-binary
