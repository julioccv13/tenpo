import json
from datetime import datetime, timedelta
from typing import Dict
import inspect
import logging
import airflow
import pendulum
import requests
from airflow.decorators import task
from airflow.models import Variable
from plugins.slack import get_task_success_slack_alert_callback

DEPLOYMENT_ENVIRONMET = Variable.get("environment")
CLOUDRUN_URL = Variable.get("cloudrun_url")
BQ_PROJECT_NAME = Variable.get("project1")
SLACK_CONN_ID = f"slack_conn-{DEPLOYMENT_ENVIRONMET}"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')

# configuration_data
configuration_data = {
    "main_python_file_uri": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/113/main.py",
    "python_file_uris": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/core.zip",
    "args": {
        "class_name": "Process113",
        "bucket_name": f"{DEPLOYMENT_ENVIRONMET}-source-files",
        "project_name": BQ_PROJECT_NAME,
        "dataset_jarvis": "jarvis",
        "bq_table": "boletin_concursal",
        "website_url":" https://www.boletinconcursal.cl/boletin/procedimientos",
        "xpath_buttom": '//*[@id="btnRegistroJson"]',
        "file_name": "registro_publicaciones_full.json",
        "var_blank_space": " ",
        "var_underscore": "_",
        "var_oP": "ó",
        "var_o":"o",
    },
}

default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 6, 6, tz="UTC"),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID),
}


with airflow.DAG(
    "113_scrapper_boletin_sucursal",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 12 * * *",
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
    compile = launch_cloud_run(configuration_data)

    compile