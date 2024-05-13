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
clevertap_account_id = Variable.get("clevertap_account_id")
clevertap_account_passcode = Variable.get("clevertap_account_passcode")
SLACK_CONN_ID = f"slack_conn-{environment}"

data = {
    "main_python_file_uri": f"gs://{environment}-source-files/008a/main.py",
    "python_file_uris": f"gs://{environment}-source-files/008a/core.zip",
    "args": {
        "period": "weekly",
        "ds_nodash":
        '{{ dag_run.conf["exec_date"] if "exec_date" in dag_run.conf else yesterday_ds_nodash }}',
        # TODO: No enviar a clevertap hasta estar certificado
        "clevertap_url": "https://api.clevertap.com/1/upload",
        "clevertap_account_id": clevertap_account_id,
        "clevertap_account_passcode": clevertap_account_passcode,
        "class_name": "Process008",
        "bucket_name": f"{environment}-source-files",
        "project_id": "tenpo-bi",
        "project_source_1": "tenpo-airflow-prod",
        "project_source_2": "tenpo-datalake-prod",
        "project_source_3": "tenpo-sandbox",
        "project_source_4": "tenpo-bi-prod",
        "project_target": "tenpo-bi",
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
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': get_task_success_slack_alert_callback(SLACK_CONN_ID),
}


with airflow.DAG(
        '008a_Clevertap_Weekly_Injection_Cloudrun',
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
