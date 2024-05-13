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
SLACK_CONN_ID = f"slack_conn-{DEPLOYMENT_ENVIRONMET}"
CLOUDRUN_URL = Variable.get("cloudrun_url")
BQ_PROJECT_NAME = f'tenpo-datalake-{DEPLOYMENT_ENVIRONMET}'
BQ_PROJECT_TARGET = f'tenpo-datalake-{DEPLOYMENT_ENVIRONMET}'
PROJECT_SOURCE_1 = Variable.get("project2")
PROJECT_SOURCE_2 = Variable.get("project1")
GA4_BASE_URL = Variable.get("ga4_base_url")
FIREBASE_APP_ID = Variable.get("firebase_app_id")
IOS_ID = Variable.get("ios_id")
ANDROID_ID = Variable.get("android_id")
IOS_API_SECRET = Variable.get("ios_api_secret")
ANDROID_API_SECRET = Variable.get("android_api_secret")

current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')
CREATE_TEMPS_TABLES_PATH = "gs://{bucket_name}/119/queries"

def launch_cloud_run(**kwargs):
    response = requests.post(kwargs['cloud_run'], json={
        "main_python_file_uri": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/119/{kwargs['section']}/main.py",
        "python_file_uris": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/core.zip",
            "args": {
                "class_name": kwargs["class_name"],
                "env": DEPLOYMENT_ENVIRONMET,
                "bucket_name": f"{DEPLOYMENT_ENVIRONMET}-source-files",
                "environment": DEPLOYMENT_ENVIRONMET,
                "project_id": BQ_PROJECT_NAME,
                "project_target": BQ_PROJECT_TARGET,
                "project_source_1": PROJECT_SOURCE_1,
                "project_source_2": PROJECT_SOURCE_2,
                "ga_base_url": GA4_BASE_URL,
                "firebase_app_id": FIREBASE_APP_ID,
                "ios_id": IOS_ID,
                "android_id": ANDROID_ID,
                "ios_api_secret": IOS_API_SECRET,
                "android_api_secret": ANDROID_API_SECRET,
                "ds_nodash": current_ds,
                "raw_query": kwargs["raw_query"]
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
with DAG("119_send_events_ga4_v2",schedule_interval="0 0 * * *",catchup=False,default_args=default_args) as dag:
    from google.cloud import storage
    client = storage.Client()
    list_of_queries = list(client.list_blobs(f'{DEPLOYMENT_ENVIRONMET}-source-files', prefix='119/queries'))
    blob_object = [blob for blob in list_of_queries]
    raw_query_1 = blob_object[0].download_as_string().decode('utf-8')
    raw_query_2 = blob_object[1].download_as_string().decode('utf-8')
    
    start_tasks = DummyOperator(task_id='start_tasks')    
    run_query = PythonOperator(
            provide_context=True,
            task_id='create_temp_target',
            python_callable=launch_cloud_run,
            op_kwargs={'cloud_run': CLOUDRUN_URL,'section':'section_1','class_name':'Process119_Calculte_Targets','raw_query':raw_query_1}
        )
    
    insert_results = PythonOperator(
            provide_context=True,
            task_id='insert_results',
            python_callable=launch_cloud_run,
            op_kwargs={'cloud_run': CLOUDRUN_URL,'section':'section_1','class_name':'Process119_Calculte_Targets','raw_query':raw_query_2}
        )

    send_request_ga4 = PythonOperator(
            provide_context=True,
            task_id='send_payloads',
            python_callable=launch_cloud_run,
            op_kwargs={'cloud_run': CLOUDRUN_URL,'section':'section_2','class_name':'Process119_Send_Payloads','raw_query': None}
        )

    end_tasks = DummyOperator(task_id='end_tasks')
    start_tasks >> run_query >> insert_results >> send_request_ga4 >> end_tasks

