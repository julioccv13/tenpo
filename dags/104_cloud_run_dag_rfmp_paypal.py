from typing import Dict
import pendulum
import requests

import airflow
from airflow.decorators import task
from airflow.models import Variable
from plugins.slack import get_task_success_slack_alert_callback

environment = Variable.get("environment")
cloudrun_url = Variable.get("cloudrun_url")
SLACK_CONN_ID = f"slack_conn-{environment}"

data = {
    "main_python_file_uri": f"gs://{environment}-source-files/104/main.py",
    "python_file_uris": f"gs://{environment}-source-files/core.zip",
    "args": {
        "class_name": "Process104",
        "ds_nodash":
        '{{ dag_run.conf["exec_date"] if "exec_date" in dag_run.conf else ds_nodash }}',
        "bucket_name": f"{environment}-source-files",
        "project_id": "tenpo-bi",
        "project_source_1": "tenpo-bi-prod",
        "project_source_2": "hallowed-port-272015",
        "project_target": "tenpo-bi",
    }
}

default_args = {
    'owner': 'Jarvis',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 7, 1, tz="UTC"),
    'email': ['jarvis@tenpo.cl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID),
}

# 04 UTC -> 00 chile
with airflow.DAG(
        '104_cloudrun_dag_rfmp_paypal',
        catchup=False,
        default_args=default_args,
        schedule_interval="0 4 1 * *") as dag:

    @task
    def launch_cloud_run(data: Dict, **kwargs):
        response = requests.post(cloudrun_url, json=data)
        if "Success" in response.text and response.status_code == 200:
            return response.text
        else:
            raise RuntimeError(response.text)

    compilar = launch_cloud_run(data)

    compilar