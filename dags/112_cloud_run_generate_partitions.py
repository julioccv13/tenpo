from datetime import timedelta
import datetime
import logging
import requests
from airflow.models import Variable
from airflow import DAG
from plugins.slack import get_task_success_slack_alert_callback
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

DEPLOYMENT_ENVIRONMET = Variable.get("environment")
SLACK_CONN_ID = f"slack_conn-{DEPLOYMENT_ENVIRONMET}"
CLOUDRUN_URL = Variable.get("cloudrun_url")
BQ_PROJECT_NAME = f'tenpo-datalake-{DEPLOYMENT_ENVIRONMET}'
BQ_PROJECT_TARGET = Variable.get("project4")
PROJECT_SOURCE_1 = Variable.get("project2")
PROJECT_SOURCE_2 = Variable.get("project3")
PROJECT_SOURCE_3 = Variable.get("project29")
PROJECT_SOURCE_4 = Variable.get("project1")

current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

def launch_cloud_run(**kwargs):
    response = requests.post(kwargs['cloud_run'], json={
        "main_python_file_uri": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/112/{kwargs['section']}/main.py",
        "python_file_uris": f"gs://{DEPLOYMENT_ENVIRONMET}-source-files/core.zip",
            "args": {
                "class_name": kwargs['class_name'],
                "env": DEPLOYMENT_ENVIRONMET,
                "bucket_name": f"{DEPLOYMENT_ENVIRONMET}-source-files",
                "environment": DEPLOYMENT_ENVIRONMET,
                "project_id": BQ_PROJECT_NAME,
                "project_target": BQ_PROJECT_TARGET,
                "project_source_1": PROJECT_SOURCE_1,
                "project_source_2": PROJECT_SOURCE_2,
                "project_source_3": PROJECT_SOURCE_3,
                "project_source_4": PROJECT_SOURCE_4,
                "ds_nodash": current_ds
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

def update_variables():
    import pandas
    dataframe = pandas.read_gbq(f"select max(bloque) as blocks from {BQ_PROJECT_TARGET}.tmp.table_final_{current_ds}")
    n_bloques = int(dataframe.at[0,'blocks'])
    Variable.update(key="clevertap_blocks", value=n_bloques)

""" Create parallel tasks dynamically """
with DAG("112_run_generate_final_table_and_partitions",schedule_interval=None,catchup=False,default_args=default_args) as dag:
    
    generate_final_table = PythonOperator(
            provide_context=True,
            task_id='run_queries_group',
            python_callable=launch_cloud_run,
            op_kwargs={'cloud_run': CLOUDRUN_URL,'section':'section_2','class_name':'Process112_Generate_Sql'}
        )
    
    generate_blocks = PythonOperator(
                task_id="create_blocks_final_table",
                python_callable=launch_cloud_run,
                provide_context=True,
                op_kwargs={'cloud_run': CLOUDRUN_URL,'section':'section_3','class_name': 'Process112_Generate_Partitions'},
    )

    update_variable = PythonOperator(
        task_id="update_blocks",
        python_callable=update_variables,
        provide_context=True
    )

    trigger_send_payload_1 = TriggerDagRunOperator(
        task_id='trigger_send_payload_1',
        trigger_dag_id='112_send_concurrent_properties_1',
        reset_dag_run=True,
    )

    generate_final_table >> generate_blocks >> update_variable >> trigger_send_payload_1