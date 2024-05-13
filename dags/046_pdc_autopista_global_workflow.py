from airflow.models import Variable
from datetime import timedelta
from typing import Dict
import airflow
import pendulum
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from plugins.slack import get_task_success_slack_alert_callback
environment = Variable.get("environment")
SLACK_CONN_ID = f"slack_conn-{environment}"

campaing_url = "http://10.110.1.45/api/v1/job/create"
headers = {'Content-Type': 'application/json'}
body = '{"campaign_id":"7b4dc55b-0c50-4f1e-8e24-edfc737abdf7","segment_sql_file_path":"gs://prod_campaign_streaming_files/segments_sql_files/pdc/20230720_autopista_global.sql"}'

def peticion():
    r = requests.post(campaing_url, data=body, headers=headers)
    if r.status_code > 399:
        raise Exception(f"Error de comunicacion con el servicio de campañas: {r.text}")
    else:
        print(r.status_code)
        print(f'Respuesta codigo: {r.text}')


default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 7, 21, tz="UTC"),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID)
}


dag = DAG(
    "046_pdc_autopista_global_workflow",
    default_args=default_args,
    schedule_interval="0 */2 21,22 7 *",
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