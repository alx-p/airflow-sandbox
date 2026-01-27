FROM apache/airflow:2.9.1-python3.9

USER airflow

RUN pip install requests psycopg2-binary
