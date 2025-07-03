from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.models import Variable
from datetime import datetime
from datetime import timedelta
from custom_operator import CustomKubernetesPodOperator
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#user_defined_schedule = Variable.get("dag_schedule", default_var="2 * * * *")     
with DAG(
    dag_id='run_java_2',
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 
    catchup=False, # 
    max_active_runs=1,  # Ensures that only one DAG run is allowed to be active at a time; next runs are skipped or queued
    tags=['java', 'kubernetes'],
) as dag:    
     run_jar_task = CustomKubernetesPodOperator(
        task_id='marcus-task-2',
        name='run-java-2',
        namespace='default',  # 
        #image='192.168.117.185:8083/java/spring-batch/spring-batch-payment-central/spring-batch-pmc-domestic-transfer:1.0.0-develop',  #
        image="ailabadau/demo-java:latest",
        

        image_pull_policy="Always",

        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        startup_timeout_seconds=300,
        execution_timeout=timedelta(minutes=3),
        #pool="pool-test",
    )
