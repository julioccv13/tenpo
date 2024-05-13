from datetime import datetime, timedelta
import json
from typing import Dict
import pendulum
import requests

import airflow
from airflow.decorators import task
from airflow.models import Variable
from plugins.slack import get_task_success_slack_alert_callback


cloudrun_url = Variable.get("cloudrun_url")
environment = Variable.get("environment")
SLACK_CONN_ID = f"slack_conn-{environment}"

if environment == 'sandbox':
    project_id= f'tenpo-datalake-{environment}'
else:
    project_id= 'tenpo-bi'

data = {
    "main_python_file_uri": f"gs://{environment}-source-files/022/main.py",
    "python_file_uris": f"gs://{environment}-source-files/core.zip",
    "args": {
        "gcs_project": 'tenpo-datalake-prod',
        "class_name": "Solicitud_Nueva_Direccion",
        "gcs_folder": 'gs://tenpo-sac/campañas_nueva_direccion/temp/',
        "bq_project": f"{project_id}",
        "bq_table": 'aux.comunicacion_solicitud_nueva_direccion_tf'
    }
}

default_args = {
    'owner': 'Jarvis',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 6, 1, tz="UTC"),
    'email': ['jarvis@tenpo.cl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID),
}


with airflow.DAG(
        '022_Solicitud_Nueva_Direccion',
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
