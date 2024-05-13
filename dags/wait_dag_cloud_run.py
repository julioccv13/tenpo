from datetime import timedelta
from typing import Dict
import airflow
import pendulum
import requests
from airflow.decorators import task
from airflow.models import Variable
import logging
from airflow.utils.dates import days_ago
from plugins.slack import get_task_success_slack_alert_callback

DEPLOYMENT_ENVIRONMET = Variable.get("environment")
PROJECT_TARGET = Variable.get("project4")
PROJECT_ID = f'tenpo-datalake-{DEPLOYMENT_ENVIRONMET}'
CLOUDRUN_URL = Variable.get("cloudrun_url")
SENDGRID_API_KEY = Variable.get('sendgrid_api_key')
SLACK_CONN_ID = f"slack_conn-{DEPLOYMENT_ENVIRONMET}"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')

data = {
    "main_python_file_uri": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/wait_dag/main.py",
    "python_file_uris": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/core.zip",
    "args": {
        "ds_nodash": '{{ dag_run.conf["exec_date"] if "exec_date" in dag_run.conf else ds_nodash }}',
        "bucket_name": f"{DEPLOYMENT_ENVIRONMET}-source-files",
        "environment": DEPLOYMENT_ENVIRONMET,
        "project_id": PROJECT_ID,
        "project_target": PROJECT_TARGET,
        "class_name": "Wait_Dag",
        "sendgrid_api_key": SENDGRID_API_KEY
    },
}


default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date":days_ago(1),
    "email": ["jarvis@tenpo.cl", "crm@tenpo.cl"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID),
}


with airflow.DAG(
    "Send_email_Sendgrid",
    catchup=False,
    default_args=default_args,
    schedule_interval="@once",
) as dag:

    @task
    def launch_cloud_run(data: Dict, **kwargs):
        response = requests.post(CLOUDRUN_URL, json=data)
        try:
            if response.status_code == 200:
                return f'Success: {response.text}'
            elif response.status_code != 200:
                logging.info(f'Response text: {response.text}, Response Reason: {response.reason} and Status Code: {response.status_code}')
                raise Exception(f'Response text: {response.text}, Response Reason: {response.reason} and Status Code: {response.status_code}')
        except Exception as e:
            logging.info(f'New error ocurred: {e} with Response Text: {response.text}')
            raise Exception(f'New error ocurred: {e} with Response Text: {response.text}')

    compile = launch_cloud_run(data)

    compile