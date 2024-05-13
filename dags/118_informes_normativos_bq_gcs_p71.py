from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime as dt
from airflow.decorators import task
import logging
import requests
from typing import Dict
from plugins.slack import get_task_success_slack_alert_callback



#Constants
CODIGO_DAG=118
CODIGO_REPORTE="p71"
DEPLOYMENT_ENVIRONMET = Variable.get("environment")
BQ_PROJECT_NAME = Variable.get("project30")
BQ_DATASET=Variable.get("cmf-dataset")
CLOUDRUN_URL = Variable.get("cloudrun_url")
BUCKET_NAME=f"{DEPLOYMENT_ENVIRONMET}-thirdparty-output-data"
EXEC_DATE="{{ ds }}"
QUERY_DATE=EXEC_DATE
HEADER_DATE="{{ execution_date.strftime('%Y%m') }}"
BLOB_NAME=f"{EXEC_DATE}-{CODIGO_REPORTE.lower()}"
SLACK_CONN_ID = f"slack_conn-{DEPLOYMENT_ENVIRONMET}"


# configuration_data
configuration_data = {
    "main_python_file_uri": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/{str(CODIGO_DAG)}/main.py",
    "python_file_uris": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/core.zip",
    "args": {
        "class_name": f"Process{str(CODIGO_DAG)}",
        "output_bucket_name":BUCKET_NAME,
        "table_path":"",
        "blob_name":BLOB_NAME,
        "exec_date":EXEC_DATE,
        "query_date":QUERY_DATE,
        "header_date":HEADER_DATE,
        "bucket_name": f"{DEPLOYMENT_ENVIRONMET}-source-files",
        "project_name":BQ_PROJECT_NAME,
        "dataset":BQ_DATASET,
        "codigo_reporte":CODIGO_REPORTE,
        "codigo_dag":CODIGO_DAG
    },
}


default_args = {
    "owner": "j.toro.externo",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 8, 20, tz="UTC"),
    "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID),
    "retries": 5,
    "retry_delay": pendulum.duration(minutes=2),
}

with DAG(
    dag_id=f"{str(CODIGO_DAG)}_informes_normativos_bq_gcs_{str(CODIGO_REPORTE)}",
    default_args=default_args,
    description='DAG to move BQ Queried data to GCS',
    start_date=pendulum.datetime(2023, 8, 20, tz="UTC"),
    schedule_interval='0 1 * * *'
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