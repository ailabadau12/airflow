from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id='run_java_image_k8s_dag',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['java', 'kubernetes'],
) as dag:

    run_jar_task = KubernetesPodOperator(
        task_id='run_java_image_task',
        name='run-java-image',
        namespace='airflow',  # đổi nếu namespace khác
        image='ailabadau/dag:0.0.1',  # image Java đã build sẵn
        # Nếu image cần CMD riêng thì thêm dòng dưới:
        cmds=["java", "-jar", "/app/demo.jar"],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
    )
