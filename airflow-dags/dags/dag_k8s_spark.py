from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'ndquoc',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

namespace = conf.get("kubernetes", "NAMESPACE")

if namespace == 'default':
    config_file = "C:/Users/nduyq/pyspark/code.yaml"
    in_cluster = False
else:
    in_cluster = True
    config_file = None

dag = DAG(
    dag_id='dag-test',
    default_args=default_args,
    schedule_interval='@once',
)
task1 = KubernetesPodOperator(
    namespace=namespace,
    image="ndquoc1306/spark:0.1.0",
    name="task-test",
    task_id="cf0001-1",
    in_cluster=in_cluster,
    cluster_context='docker-desktop',
    config_file=config_file,
    is_delete_operator_pod=True,
    get_logs=True
)

task1
