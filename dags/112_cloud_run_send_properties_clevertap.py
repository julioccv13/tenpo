from datetime import timedelta
import datetime
import logging
import requests
from airflow.models import Variable
from airflow import DAG
from plugins.slack import get_task_success_slack_alert_callback
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DEPLOYMENT_ENVIRONMET = Variable.get("environment")
CLOUDRUN_URL = Variable.get("cloudrun_url")
BQ_PROJECT_NAME = f'tenpo-datalake-{DEPLOYMENT_ENVIRONMET}'
BQ_PROJECT_TARGET = Variable.get("project4")
CLEVERTAP_ACCOUNT_ID = Variable.get("clevertap_account_id")
CLEVERTAP_ACCOUNT_PASSCODE = Variable.get("clevertap_account_passcode")
CLEVERTAP_BASE_URL = Variable.get("clevertap_base_url")
CT_PROFILES_MAX_RECORDS_PER_PAYLOAD = 1000
SLACK_CONN_ID = f"slack_conn-{DEPLOYMENT_ENVIRONMET}"
CREATE_TEMPS_TABLES_PATH = "gs://{bucket_name}/112/queries"
PARALEL_TASKS = 6
CLEVERTAP_BLOCKS = int(Variable.get("clevertap_blocks"))
current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

def launch_cloud_run(**kwargs):
    response = requests.post(kwargs['cloud_run'], json={
        "main_python_file_uri": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/112/section_4/main.py",
        "python_file_uris": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/core.zip",
            "args": {
                "class_name": "Process112_Send_Payloads",
                "bucket_name": f"{DEPLOYMENT_ENVIRONMET}-source-files",
                "environment": DEPLOYMENT_ENVIRONMET,
                "project_id": BQ_PROJECT_NAME,
                "project_target": BQ_PROJECT_TARGET,
                "ds_nodash": "{{yesterday_ds_nodash}}",
                "clevertap_account_id": CLEVERTAP_ACCOUNT_ID,
                "clevertap_account_passcode": CLEVERTAP_ACCOUNT_PASSCODE,
                "clevertap_base_url": CLEVERTAP_BASE_URL,
                "custom_number": kwargs['customer_number'],
                "index": kwargs['index'],
                "interval": 100000
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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID),
}

""" Create parallel tasks dynamically """
def create_dag(dag_id, schedule, default_args,index):
    dag = DAG(dag_id,schedule_interval=schedule,default_args=default_args)
    with dag:
        start_task = DummyOperator(task_id=f'begin_payload_{index}')
        def create_dynamic_task(current_task_number):
            return PythonOperator(
                provide_context=True,
                task_id='send_payload_' + str(current_task_number),
                python_callable=launch_cloud_run,
                op_kwargs={'cloud_run': CLOUDRUN_URL, 'customer_number': current_task_number,'index':index},
            )
        next_task = TriggerDagRunOperator(task_id=f"trigger_send_payload_{index+1}", trigger_dag_id =f'112_send_concurrent_properties_{index+1}',reset_dag_run=True)
        for task in range(1,PARALEL_TASKS):
            created_task = create_dynamic_task(task)
            start_task >> created_task
            created_task >> next_task
    return dag

""" Create dags dynamically """
for index in range(1, CLEVERTAP_BLOCKS+1):
    dag_id = '112_send_concurrent_properties_{}'.format(str(index))
    schedule = None
    bloque = index
    globals()[dag_id] = create_dag(dag_id,
                                    schedule,
                                    default_args,
                                    bloque)
""" Finish dags """
with DAG(dag_id=f"112_send_concurrent_properties_{CLEVERTAP_BLOCKS+1}", default_args=default_args,schedule_interval=None,max_active_runs=1) as dag:
            end_tasks = DummyOperator(task_id=f'this_is_the_end')
