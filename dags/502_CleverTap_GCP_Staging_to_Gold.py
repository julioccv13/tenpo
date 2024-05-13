from imaplib import Commands
import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule


from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
    DataprocListBatchesOperator,
)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator

PROJECT_ID = "tenpo-datalake-prod"
REGION = "us-central1"
BATCH_ID = "502-batch"
SOURCE_DEV_BUCKET = 'prod-source-files'
SOURCE_BUCKET = "clevertap_staging"
TARGET_BUCKET = "clevertap_gold"
DATE_TO_PROCESS = '{{ dag_run.conf["exec_date"] if "exec_date" in dag_run.conf else yesterday_ds }}'
BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": f"gs://{SOURCE_DEV_BUCKET}/502/main.py",
        "python_file_uris": [f"gs://{SOURCE_DEV_BUCKET}/502/core.zip"],
        "args": [f"-PSP=gs://{SOURCE_BUCKET}/dt={DATE_TO_PROCESS}/*",
                 f"-PGF=gs://{TARGET_BUCKET}/dt={DATE_TO_PROCESS}"],
    },
}

with models.DAG(
    "502_CleverTap_GCP_Staging_to_Gold",
    schedule_interval="0 14 * * *",
    start_date=pendulum.datetime(2022, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs= 4,
) as dag_batch:
    # [START how_to_cloud_dataproc_create_batch_operator]

    @task
    def check_files(date, **kwargs):
        from google.cloud import storage
        print("Checking files for", date)
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(f'{SOURCE_BUCKET}',
                                          prefix=f'dt={date}/',
                                          max_results=1)
        if not list(blobs):
            raise AirflowSkipException('No hay archivos para procesar')

    @task
    def check_files_target(date, **kwargs):
        from google.cloud import storage
        print("Checking files in target folder for", date)
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(f'{TARGET_BUCKET}',
                                          prefix=f'dt={date}/')
        for blob in blobs:
            blob.delete()
        
    check_data_in_source = check_files(DATE_TO_PROCESS)
    delete_files_in_target = check_files_target(DATE_TO_PROCESS)

    
    
    # [START how_to_cloud_dataproc_list_batches_operator]
    list_batches = DataprocListBatchesOperator(
        task_id="list_batches",
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_list_batches_operator]

    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region="{{ ti.xcom_pull(key='return_value', task_ids='bash_operator') }}",
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
    )
    # [END how_to_cloud_dataproc_create_batch_operator]

    # [START how_to_cloud_dataproc_get_batch_operator]
    get_batch = DataprocGetBatchOperator(
        task_id="get_batch", project_id=PROJECT_ID, region="{{ ti.xcom_pull(key='return_value', task_ids='bash_operator') }}", batch_id=BATCH_ID
    )
    # [END how_to_cloud_dataproc_get_batch_operator]



    # [START how_to_cloud_dataproc_delete_batch_operator]
    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch", project_id=PROJECT_ID, region="{{ ti.xcom_pull(key='return_value', task_ids='bash_operator') }}", batch_id=BATCH_ID, trigger_rule=TriggerRule.ALL_DONE
    )
    
    commands = """
        sleep 65
        zones="us-east1 us-west1 us-west2 us-central1"
        for val in $zones; do
            if gcloud dataproc batches list --region=$val | grep -q "RUNNING"; then
                echo "There are running batches in $val"
            else
                echo "$val"
                break
            fi
            
        done
        """
    bash_operator = BashOperator(
        task_id='bash_operator',
        bash_command=commands,
        pool="choose_zone_pool",
        do_xcom_push=True,
    )

    # [END how_to_cloud_dataproc_delete_batch_operator]

    check_data_in_source >> delete_files_in_target >> list_batches >> bash_operator >> create_batch >> get_batch >> delete_batch
    
    
    
