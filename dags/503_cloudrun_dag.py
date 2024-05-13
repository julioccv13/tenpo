from datetime import timedelta
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
    "main_python_file_uri": f"gs://{environment}-source-files/503/main.py",
    "python_file_uris": f"gs://{environment}-source-files/core.zip",
    "args": {
        "ds": '{{ ds }}',
        "clevertap_base_url": Variable.get("clevertap_base_url"),
        "bucket_name": f"{environment}-source-files",
        "clevertap_account_id": Variable.get("clevertap_account_id"),
        "clevertap_passcode": Variable.get("cloudrun_url"),
        "project1": Variable.get("project1"),
        "project_id": f"tenpo-datalake-{environment}",
        "project3": Variable.get("project3"),
        "project4": Variable.get("project4"),
        "project9": Variable.get("project9"),
        "env": environment,
        "class_name": "Process503",
    }
}

default_args = {
    'owner': 'Jarvis',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 12, 18, tz="UTC"),
    'email': ['jarvis@tenpo.cl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID),
}


with airflow.DAG(
        '503_cloudrun_dag',
        catchup=False,
        default_args=default_args,
        schedule_interval="0 0 * * *"
) as dag:

    @task
    def launch_cloud_run(data: Dict, **kwargs):
        response = requests.post(cloudrun_url, json=data)
        if "Success" in response.text and response.status_code == 200:
            return response.text
        else:
            raise RuntimeError(response.text)

    compilar = launch_cloud_run(data)

    compilar
