import json
import pandas as pd
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import time

clevertap_url = Variable.get("clevertap_url")
clevertap_presigned_url = Variable.get("clevertap_presigned_url")
clevertap_base_url = Variable.get("clevertap_base_url")
zendesk_api = Variable.get("zendesk_api")
cloudrun_url = Variable.get("cloudrun_url")

# Definicion del dag
default_args_daily = {
    "owner": "JARVIS",
    "depends_on_past": False,
    "start_date": datetime(2023, 4, 27),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": "adriana.abad@tenpo.cl",
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=1),
}

# Define DAG:
dag = DAG(
    "dag_infinity",
    default_args=default_args_daily,
    schedule_interval="@once",
    max_active_runs=1,
)

def print_values():
    time.sleep(2)
    return f'hello world'

def raiseError():
    raiseError(f'Oh boy!')
      
print_values = PythonOperator(
    task_id="print_values",
    python_callable=print_values,
    provide_context=True,
    email_on_failure=True,
    email='jarvis@tenpo.cl',
    dag=dag,
)

raise_error = PythonOperator(
    task_id="raise_error",
    python_callable=raiseError,
    provide_context=True,
    email_on_failure=True,
    email='jarvis@tenpo.cl',
    dag=dag,
)

print_values >> raise_error
