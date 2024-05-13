from datetime import timedelta, datetime
from typing import Dict
import pendulum
import requests

from google.cloud import storage

import airflow
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

environment = Variable.get("environment")
project_target = f"tenpo-datalake-{environment}"
jobs_datetime_name = str(datetime.now().strftime("%Y%m%d-%H%M%S"))

default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2022, 6, 1, tz="UTC"),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with airflow.DAG(
    "0001_recomendation-system",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * 1",
    max_active_runs=1,
    user_defined_macros={
        "project_target": project_target,  # Macro can be a variable
        "jobs_datetime_name": jobs_datetime_name,
    },
) as dag:

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command='gcloud beta run jobs create recomendation-system-data-ingestion-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomendation-system \
            --tasks 1 \
                --set-env-vars PROCESS="ingesta_datos" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomendation-system" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute recomendation-system-data-ingestion-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    data_validation = BashOperator(
        task_id="data_validation",
        bash_command='gcloud beta run jobs create recomendation-system-data-validation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomendation-system \
            --tasks 1 \
            --set-env-vars PROCESS="validacion_data" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomendation-system" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                && gcloud beta run jobs execute recomendation-system-data-validation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    model_inference = BashOperator(
        task_id="model_inference",
        bash_command='gcloud beta run jobs create recomendation-system-model-inference-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomendation-system \
            --tasks 1 \
            --set-env-vars PROCESS="modelamiento" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomendation-system" \
                                    --region us-east1 --memory=24576M  --cpu=8 \
                && gcloud beta run jobs execute recomendation-system-model-inference-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    (data_ingestion >> data_validation >> model_inference)
    data_ingestion
