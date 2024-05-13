from datetime import timedelta
from typing import Dict
import pendulum
import requests

import airflow
from airflow.decorators import task
from airflow.models import Variable

environment = Variable.get("environment")
cloudrun_url = Variable.get("cloudrun_url")
typeform_account_passcode = Variable.get("typeform_account_passcode")


data = {
    "main_python_file_uri": f"gs://{environment}-source-files/P15/main.py",
    "python_file_uris": f"gs://{environment}-source-files/core.zip",
    "args": {
        "url": "https://api.typeform.com",
        "typeform_account_passcode": typeform_account_passcode,
        "since": '{{ dag_run.conf["since_date"] if "since_date" in dag_run.conf else macros.ds_add(ds, -2) }}T00:00:00',
        "until": '{{ dag_run.conf["until_date"] if "until_date" in dag_run.conf else ds }}T00:59:59',
        "bucket_name": f"{environment}_typeform",
        "project_id": "tenpo-external",
        "class_name": "ProcessP15"
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
    'retry_delay': timedelta(minutes=1)
}


with airflow.DAG(
    'P15_cloudrun_dag',
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
