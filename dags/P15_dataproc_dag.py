import pendulum

from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
    DataprocListBatchesOperator,
)


environment = Variable.get("environment")
cloudrun_url = Variable.get("cloudrun_url")
typeform_account_passcode = "2KfodgTuZiQuFTWaxf3hMSR2yD9mKfDWpHLCqkusTkHG"  #Variable.get("typeform_account_passcode")
PROJECT_ID = "tenpo-datalake-prod"
REGION = "us-central1"
BATCH_ID = "p15-batch"
SOURCE_DEV_BUCKET = 'prod-source-files'


BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": f"gs://{environment}-source-files/P15/main.py",
        "python_file_uris": [f"gs://{environment}-source-files/core.zip"],
        "args": [
            "--url=https://api.typeform.com",
            f"--typeform-account-passcode={typeform_account_passcode}",
            '--since={{ dag_run.conf["since_date"] if "since_date" in dag_run.conf else macros.ds_add(ds, -2) }}T00:00:00',
            '--until={{ dag_run.conf["until_date"] if "until_date" in dag_run.conf else ds }}T00:59:59',
            f"--bucket-name={environment}_typeform",
            "--project-id=tenpo-external"
        ]
    }
}

with models.DAG(
    "P15_Typeform_Dataproc",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2022, 6, 1, tz="UTC"),
    catchup=False,
) as dag_batch:
    # [START how_to_cloud_dataproc_create_batch_operator]

    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
    )
    # [END how_to_cloud_dataproc_create_batch_operator]

    # [START how_to_cloud_dataproc_get_batch_operator]
    get_batch = DataprocGetBatchOperator(
        task_id="get_batch", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID
    )
    # [END how_to_cloud_dataproc_get_batch_operator]

    # [START how_to_cloud_dataproc_list_batches_operator]
    list_batches = DataprocListBatchesOperator(
        task_id="list_batches",
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_list_batches_operator]

    # [START how_to_cloud_dataproc_delete_batch_operator]
    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch", project_id=PROJECT_ID, region=REGION, batch_id=BATCH_ID, trigger_rule=TriggerRule.ALL_DONE
    )
    # [END how_to_cloud_dataproc_delete_batch_operator]

    create_batch >> get_batch >> list_batches >> delete_batch
