from datetime import datetime, timedelta
from typing import Dict

import airflow
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

from plugins.slack import get_task_success_slack_alert_callback

environment = Variable.get("environment")
cloudrun_url = Variable.get("cloudrun_url")
SLACK_CONN_ID = f"slack_conn-{environment}"


data = {
    "main_python_file_uri": f"gs://{environment}-source-files/101/main.py",
    "python_file_uris": f"gs://{environment}-source-files/core.zip",
    "args": {
        "ds": "{{ ds }}",
        "ds_nodash": "{{ ds_nodash }}",
        "bucket_name": f"{environment}-source-files",
        "bucket_name_input": f"external_selyt_input",
        "bucket_name_output": f"external_selyt_output",
        "filename": "seguimiento_tenpo_selyt.xlsx",
        "output": "seguimiento_tenpo_selyt",
        "project_id": f"tenpo-datalake-{environment}",
        "project_target": f"tenpo-datalake-{environment}",
        "project3": Variable.get("project3"),
        "project2": Variable.get("project2"),
        "env": f"{environment}",
        "class_name": "Process101",
        "cloudrun": f"{cloudrun_url}"
    },
}

default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["adriana.abad@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID)
}

with airflow.DAG(
    "101_cloudrun_dag_selyt_integration",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 12,14,16,18,20,22,0 * * *",
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
