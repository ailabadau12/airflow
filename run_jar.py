from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import subprocess
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

jar_path = "/jar/demo-0.0.1-SNAPSHOT.jar"
cmd = ['java', '-jar', jar_path]  # Đơn giản hóa lệnh như bạn muốn

def run_jar():
    logging.info(f"Đang khởi động JAR: {jar_path}")
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    def stream_output(stream, prefix):
        for line in iter(stream.readline, ''):
            if line:
                logging.info(f"{prefix} {line.strip()}")

    stdout_thread = threading.Thread(target=stream_output, args=(process.stdout, "[JAR STDOUT]"))
    stderr_thread = threading.Thread(target=stream_output, args=(process.stderr, "[JAR STDERR]"))

    stdout_thread.start()
    stderr_thread.start()

    # Chờ tiến trình kết thúc
    process.wait()

    # Đảm bảo đọc hết output
    stdout_thread.join()
    stderr_thread.join()

    if process.returncode == 0:
        logging.info("JAR đã chạy thành công")
    else:
        logging.error(f"JAR kết thúc với mã lỗi {process.returncode}")

with DAG(
    dag_id='run_java_jar_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['java', 'jar'],
) as dag:

    run_jar_task = PythonOperator(
        task_id='run_jar_task',
        python_callable=run_jar,
    )
