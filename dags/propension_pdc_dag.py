from datetime import timedelta, datetime
from typing import Dict
import pendulum
import requests
import pandas as pd

from google.cloud import storage

import airflow
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
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


def find_first_monday(year, month, day):
    d = datetime(year, int(month), 7)
    offset = -d.weekday()  # weekday = 0 means monday
    return d + timedelta(offset)


def check_retraing_schedule(retraining_day, day, repo_name, ds_nodash):
    path_models = f"models/{repo_name}/saved_models/current_model/modelo.joblib"
    bucket_name = f"jarvis_mlops_{environment}"
    storage_client = storage.Client(project=project_target)
    bucket = storage_client.bucket(bucket_name)

    dt = datetime.strptime(ds_nodash, "%Y%m%d")
    date = datetime.strptime(ds_nodash, "%Y%m%d")

    # Entrenamiento el primer lunes de cada mes
    if retraining_day == "monthly":
        check = (day == "1") | ((storage.Blob(bucket=bucket, name=path_models).exists()) == False)
        # check = (date == find_first_monday(dt.year, dt.month, dt.day)) | (
        #     (storage.Blob(bucket=bucket, name=path_models).exists()) == False
        # )
    # Entrenamiento diario
    elif retraining_day == "daily":
        print("Entrenar modelo:")
        check = True
    # if is not monthlly or daily check per day
    else:
        check = retraining_day == day
    return check


def choose_branch(**kwargs):
    date = datetime.strptime(kwargs["templates_dict"]["ds_nodash"], "%Y%m%d")
    day = str(date.day)
    if check_retraing_schedule(
        kwargs["templates_dict"]["retraining_day"],
        day,
        repo_name=kwargs["templates_dict"]["repo_name"],
        ds_nodash=kwargs["templates_dict"]["ds_nodash"],
    ):
        return ["retraining_pipeline"]
    else:
        return ["inference_pipeline"]


with airflow.DAG(
    "0001_propension-pdc",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 2 * * *",
    max_active_runs=1,
    user_defined_macros={
        "project_target": project_target,  # Macro can be a variable
        "jobs_datetime_name": jobs_datetime_name,
    },
) as dag:

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        templates_dict={
            "ds_nodash": "{{ds_nodash}}",
            "retraining_day": "monthly",  # Entrenamiento mensual por defecto
            "repo_name": "propension-pdc",
        },
    )

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command='gcloud beta run jobs create propension-pdc-data-ingestion-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propension-pdc \
            --tasks 1 \
                --set-env-vars PROCESS="data_ingestion" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propension-pdc" \
                                    --region us-east1 --memory=8192M  --cpu=4 --task-timeout=3600 \
                    && gcloud beta run jobs execute propension-pdc-data-ingestion-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    data_validation = BashOperator(
        task_id="data_validation",
        bash_command='gcloud beta run jobs create propension-pdc-data-validation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propension-pdc \
            --tasks 1 \
            --set-env-vars PROCESS="data_validation" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propension-pdc" \
                                    --region us-east1 --memory=8192M  --cpu=4 --task-timeout=3600 \
                && gcloud beta run jobs execute propension-pdc-data-validation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )
    data_preparation = BashOperator(
        task_id="data_preparation",
        bash_command='gcloud beta run jobs create propension-pdc-data-preparation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propension-pdc \
            --tasks 1 \
            --set-env-vars PROCESS="data_preparation" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propension-pdc" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute propension-pdc-data-preparation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    model_training = BashOperator(
        task_id="model_training",
        bash_command='gcloud beta run jobs create propension-pdc-model-training-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propension-pdc \
            --tasks 1 \
            --set-env-vars PROCESS="model_training" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propension-pdc" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute propension-pdc-model-training-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    model_validation = BashOperator(
        task_id="model_validation",
        bash_command='gcloud beta run jobs create propension-pdc-model-validation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propension-pdc \
            --tasks 1 \
            --set-env-vars PROCESS="model_validation" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propension-pdc" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute propension-pdc-model-validation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    data_inference = BashOperator(
        task_id="data_inference",
        bash_command='gcloud beta run jobs create propension-pdc-data-prep-infe-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propension-pdc \
            --tasks 1 \
            --set-env-vars PROCESS="data_inference" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propension-pdc" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                && gcloud beta run jobs execute propension-pdc-data-prep-infe-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference = BashOperator(
        task_id="inference",
        bash_command='gcloud beta run jobs create propension-pdc-inference-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propension-pdc \
            --tasks 1 \
            --set-env-vars PROCESS="inference" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propension-pdc" \
                                    --region us-east1 --memory=8192M  --cpu=4 --task-timeout=1800 \
                && gcloud beta run jobs execute propension-pdc-inference-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    retraining_pipeline = DummyOperator(
        task_id="retraining_pipeline",
    )
    inference_pipeline = DummyOperator(
        task_id="inference_pipeline",
    )

    join = DummyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    (
        branching
        >> retraining_pipeline
        >> data_ingestion
        >> data_validation
        >> data_preparation
        >> model_training
        >> model_validation
        >> join
    )
    branching >> inference_pipeline >> join
    join >> data_inference >> inference
