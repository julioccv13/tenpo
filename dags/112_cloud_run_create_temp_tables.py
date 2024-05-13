"""
Author: Adriana Abad
Purpose: This Process is designed to update our CRM properties using CloudRuns

It's divided in 3 DAGs and 4 main scripts, each DAG is linked to each other using the TriggerDagRunOperator
    Part I: 112_cloud_run_create_temp_tables.py
        The first step is to create all the temporary tables listed in the bucket
    
    Part II: 112_cloud_run_generate_partitions.py
        The second step is from all the temporary tables create one big final table using the previous temp tables created in the first part
        Once the final table is created, we create small chunks of 500K records each
    
    Part III: 112_cloud_run_send_properties_clevertap.py
        The third step is to grab each chunk of 500K and send the requests to clevertap
        Each request can send 1000 users and we can send up to 15 parallel request per second
        In total, we'll perform 3 iterations:
            - First we split the big final table in chunks of 500K
            - Second each chunk of 500K we'll split into 5 partitioned tables of 100K rows each
            - Third and final, each final chunk of 100K we start looping every 1000 records to send it finally to clevertap
"""

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
from google.cloud import storage

DEPLOYMENT_ENVIRONMET = Variable.get("environment")
SLACK_CONN_ID = f"slack_conn-{DEPLOYMENT_ENVIRONMET}"
CLOUDRUN_URL = Variable.get("cloudrun_url")
BQ_PROJECT_NAME = f'tenpo-datalake-{DEPLOYMENT_ENVIRONMET}'
BQ_PROJECT_TARGET = Variable.get("project4")
PROJECT_SOURCE_1 = Variable.get("project2")
PROJECT_SOURCE_2 = Variable.get("project3")
PROJECT_SOURCE_3 = Variable.get("project29")
PROJECT_SOURCE_4 = Variable.get("project1")
PARALLEL_TASKS = 6

current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

def launch_cloud_run(**kwargs):
    response = requests.post(kwargs['cloud_run'], json={
        "main_python_file_uri": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/112/section_1/main.py",
        "python_file_uris": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/core.zip",
            "args": {
                "class_name": "Process112_Calculate_Queries",
                "env": DEPLOYMENT_ENVIRONMET,
                "bucket_name": f"{DEPLOYMENT_ENVIRONMET}-source-files",
                "environment": DEPLOYMENT_ENVIRONMET,
                "project_id": BQ_PROJECT_NAME,
                "project_target": BQ_PROJECT_TARGET,
                "project_source_1": PROJECT_SOURCE_1,
                "project_source_2": PROJECT_SOURCE_2,
                "project_source_3": PROJECT_SOURCE_3,
                "project_source_4": PROJECT_SOURCE_4,
                "ds_nodash": "{{yesterday_ds_nodash}}",
                "custom_number": kwargs['custom_number'],
                "range_interval": kwargs['range_interval']
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
with DAG("112_create_queries_clevertap",schedule_interval="05 06 * * *",catchup=False,default_args=default_args) as dag:
    client = storage.Client()
    list_of_queries = len(list(client.list_blobs(f'{DEPLOYMENT_ENVIRONMET}-source-files', prefix='112/queries')))
    def split(list, n):
            k, m = divmod(len(list), n)
            return (list[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
    """Create a nested list with the indexes inside to run quieres in parallel tasks"""
    intervals = list(split([x for x in range(list_of_queries)], PARALLEL_TASKS))

    def create_dynamic_task(current_task_number):
        return PythonOperator(
            provide_context=True,
            task_id='run_queries_group_' + str(current_task_number),
            python_callable=launch_cloud_run,
            op_kwargs={'cloud_run': CLOUDRUN_URL,'range_interval': intervals[current_task_number],'custom_number': current_task_number},
        )
    start_tasks = DummyOperator(task_id='start_tasks')
    """Trigger Next DAG (112_cloud_run_generate_partitions)"""
    trigger_generate_partitions= TriggerDagRunOperator(task_id='trigger_partitions_dag', trigger_dag_id='112_run_generate_final_table_and_partitions',reset_dag_run=True)
    for page in range(PARALLEL_TASKS):
        created_task = create_dynamic_task(page)
        start_tasks >> created_task
        created_task >> trigger_generate_partitions