import json
from datetime import datetime, timedelta
from typing import Dict
import airflow
import pendulum
import requests
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from plugins.slack import get_task_success_slack_alert_callback

environment = Variable.get("environment")
cloudrun_url = Variable.get("cloudrun_url")
project_source_1 = Variable.get("project23")
project_source_2 = Variable.get("project3")
project_source_3 = Variable.get("project16")
project_target = Variable.get("project23")
SLACK_CONN_ID = f"slack_conn-{environment}"

data = {
    "main_python_file_uri": f"gs://{environment}-source-files/045/main.py",
    "python_file_uris": f"gs://{environment}-source-files/core.zip",
    "args": {
        "class_name": "Process045",
        "ds_nodash": '{{ dag_run.conf["exec_date"] if "exec_date" in dag_run.conf else ds_nodash }}',
        "ds": '{{ dag_run.conf["exec_date_ds"] if "exec_date_ds" in dag_run.conf else ds }}',
        "bucket_name": f"{environment}-source-files",
        "project_id": project_target,
        "project_source_1": project_source_1,
        "project_source_2": project_source_2,
        "project_source_3": project_source_3,
        "project_target": project_target,
    },
}

default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID),
}


with airflow.DAG(
    "045_cloudrun_dag_notifications_hist_14",
    catchup=True,
    default_args=default_args,
    schedule_interval="0 */12 * * *",
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