from datetime import timedelta
from typing import Dict
import logging
import airflow
import pendulum
import requests
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from plugins.slack import get_task_success_slack_alert_callback
from airflow import DAG
from airflow.utils.dates import days_ago

environment = Variable.get("environment")
cloudrun_url = Variable.get("cloudrun_url")
project_id = f'tenpo-datalake-{environment}'
# SLACK_CONN_ID = f"slack_conn-{environment}"

def launch_cloud_run(**kwargs):
    response = requests.post(kwargs['cloud_run'], json={
        "main_python_file_uri": f"gs://{environment}-source-files/903_QA_base_monitoring/connect_db/main.py",
        "python_file_uris": f"gs://{environment}-source-files/core.zip",
            "args": {
                "class_name": "ProcessQABase",
                "env": environment,
                "bucket_name": f"{environment}-source-files",
                "environment": environment,
                "project_id": project_id,
                "database": kwargs["database"],
                "table_name": kwargs["table"],
                "host": kwargs["host"],
                "user": kwargs["user"],
                "password": kwargs["password"]
            }
        }
    )
    try:
        if response.status_code == 200:
            return f'Success: {response.text}'
        elif response.status_code != 200:
            logging.info(f'Response text: {response.text}, Response Reason: {response.reason} and Status Code: {response.status_code}')
            raise Exception(f'Response text: {response.text}, Response Reason: {response.reason} and Status Code: {response.status_code}')
    except Exception as e:
        logging.info(f'New error ocurred: {e} with Response Text: {response.text}')
        raise Exception(f'New error ocurred: {e} with Response Text: {response.text}')


default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID),
}

with DAG("903_QA_base_monitoring_v2",schedule_interval=None,catchup=False,default_args=default_args) as dag:
    
    connect_capa_b = PythonOperator(
            provide_context=True,
            task_id='connect_capa_b',
            python_callable=launch_cloud_run,
            op_kwargs={
                'cloud_run': cloudrun_url,
                'database':'payment_loyalty',
                'table':'missions',
                'host': Variable.get(f"host_capa_b_{environment}"),
                'user': Variable.get(f"user_capa_b_{environment}"),
                'password': Variable.get(f"password_capa_b_{environment}")
                }
        )

    connect_capa_b_users = PythonOperator(
            provide_context=True,
            task_id='connect_capa_b_users',
            python_callable=launch_cloud_run,
            op_kwargs={
                'cloud_run': cloudrun_url,
                'database':'users',
                'table':'users',
                'host': Variable.get(f"host_capa_b_users_{environment}"),
                'user': Variable.get(f"user_capa_b_users_{environment}"),
                'password': Variable.get(f"password_capa_b_users_{environment}")
                }
        )
    
    connect_capa_cca = PythonOperator(
                task_id="connect_capa_cca",
                python_callable=launch_cloud_run,
                provide_context=True,
                op_kwargs={
                'cloud_run': cloudrun_url,
                'database':'payment_cca',
                'table':'bank',
                'host': Variable.get(f"host_capa_cca_{environment}"),
                'user': Variable.get(f"user_capa_cca_{environment}"),
                'password': Variable.get(f"password_capa_cca_{environment}")
                }
    )

    connect_capa_c = PythonOperator(
                task_id="connect_capa_c",
                python_callable=launch_cloud_run,
                provide_context=True,
                op_kwargs={
                    'cloud_run': cloudrun_url,
                    'database':'prepago_api',
                    'table':'prp_movimiento',
                    'host': Variable.get(f"host_capa_c_{environment}"),
                    'user': Variable.get(f"user_capa_c_{environment}"),
                    'password': Variable.get(f"password_capa_c_{environment}")
                }
    )

    connect_capa_credits = PythonOperator(
                task_id="connect_capa_credits",
                python_callable=launch_cloud_run,
                provide_context=True,
                op_kwargs={
                    'cloud_run': cloudrun_url,
                    'database':'credit_card_transaction',
                    'table':'credit_transaction',
                    'host': Variable.get(f"host_capa_credits_{environment}"),
                    'user': Variable.get(f"user_capa_credits_{environment}"),
                    'password': Variable.get(f"password_capa_credits_{environment}")
                }
    )

    connect_capa_b >> connect_capa_b_users >> connect_capa_cca >> connect_capa_c >> connect_capa_credits