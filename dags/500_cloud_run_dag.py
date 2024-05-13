from datetime import datetime, timedelta
import json
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
    "main_python_file_uri": f"gs://{environment}-source-files/500/main.py",
    "python_file_uris": f"gs://{environment}-source-files/core.zip",
    "args": {
        "class_name": "Process500",
        "ds_nodash":
        '{{ dag_run.conf["exec_date"] if "exec_date" in dag_run.conf else ds_nodash }}',
        "bucket_name": f"{environment}-source-files",
        "project_id": "tenpo-bi",
        "project_source": "tenpo-bi-prod",
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
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID),
}


with airflow.DAG(
        '500_cloudrun_dag_mau_type',
        catchup=False,
        default_args=default_args,
        schedule_interval="0 0 * * *") as dag:

    @task
    def launch_cloud_run(data: Dict, **kwargs):
        response = requests.post(cloudrun_url, json=data)
        if "Success" in response.text and response.status_code == 200:
            return response.text
        else:
            raise RuntimeError(response.text)

    compilar = launch_cloud_run(data)

    compilar