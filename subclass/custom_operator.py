from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes import client, config
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class CustomKubernetesPodOperator(KubernetesPodOperator):
    def on_kill(self):
        try:
            # Nếu chạy trong cluster (production)
            config.load_incluster_config()

            api_instance = client.CoreV1Api()

            pod_name = self.name
            namespace = self.namespace or "default"

            # Gọi xoá pod
            api_instance.delete_namespaced_pod(
                name=pod_name,
                namespace=namespace,
                body=client.V1DeleteOptions(grace_period_seconds=0),
            )
            logging.info(f"✅ Đã xóa pod: {pod_name} trong namespace: {namespace}")
        except Exception as e:
            logging.warning(f"⚠️ Lỗi khi xoá pod: {e}")