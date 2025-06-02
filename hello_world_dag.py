from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

def print_hello():
    logging.info("Hello World – Xin chào thế giới")
    logging.warn("Hello World – Xin chào mọi người")

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
}

# Khởi tạo DAG
with DAG(
    dag_id='hello_world_every_10s',
    description='DAG in Hello World mỗi 10 giây',
    default_args=default_args,
    start_date=datetime(2025, 5, 29),
    schedule=None,  # Lặp lại mỗi 10s
    catchup=False,  # Không chạy backlog
    tags=['example'],
) as dag:

    # Task duy nhất
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    hello_task  # Chạy task này
