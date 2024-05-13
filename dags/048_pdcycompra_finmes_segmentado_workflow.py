from airflow.models import Variable
from datetime import timedelta
from typing import Dict
import airflow
import pendulum
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from plugins.slack import get_task_success_slack_alert_callback
import logging

environment = Variable.get("environment")
SLACK_CONN_ID = f"slack_conn-{environment}"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
campaing_url = "http://10.110.1.45/api/v1/job/create"
headers = {'Content-Type': 'application/json'}
body = '{"campaign_id":"eee8c40d-003d-4bd1-931e-1796e3028901","segment_sql_file_path":"gs://prod_campaign_streaming_files/segments_sql_files/pdc/20230731_compraypdc_segmentado.sql"}'

def peticion():
    r = requests.post(campaing_url, data=body, headers=headers)
    if r.status_code == 200:
        logging.info(f'Response text: {r.text} and Status Code: {r.status_code}')
    elif r.status_code == 404 and r.text in 'No se ha obtuvo resultados en el segmento':
        logging.info(f'Response text: {r.text}, Response Reason: {r.reason} and Status Code: {r.status_code}')
    elif r.status_code > 499:
        raise Exception(f"Error de comunicacion con el servicio de campañas: {r.text}")
    else:
        raise Exception(f'An error ocurred: {r.status_code} with status Code: {r.text}')

default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 7, 31, tz="UTC"),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID)
}


dag = DAG(
    "048_pdcycompra_finmes_segmentado_workflow",
    default_args=default_args,
    schedule_interval="0 */2 31,1 7-8 *",
    max_active_runs=1,
)


request_campaings = PythonOperator(
      task_id="request",
      python_callable=peticion,
      provide_context=True,
    email_on_failure=True,
    email='jarvis@tenpo.cl',
    dag=dag,
)

request_campaings