from datetime import timedelta, datetime
from typing import Dict
import pendulum
import requests
import pandas as pd
from google.cloud import storage

import airflow
from airflow.decorators import task
from airflow.models import Variable, DAG
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
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    print("ckeck")

    dt = datetime.strptime(ds_nodash, "%Y%m%d")
    date = datetime.strptime(ds_nodash, "%Y%m%d")

    # check if execution day equal 1
    if retraining_day == "monthly":
        check = (date == find_first_monday(dt.year, dt.month, dt.day)) | (
            (storage.Blob(bucket=bucket, name=path_models).exists()) == False
        )
    # if is daily always true
    elif retraining_day == "daily":
        print("Entrenar modelo:")
        check = True
    # if is not monthlly or daily check per day
    else:
        True  # check = retraining_day == day
    return check


def choose_branch(**kwargs):
    date = datetime.strptime(kwargs["templates_dict"]["ds_nodash"], "%Y%m%d")
    day = str(date.day)
    if check_retraing_schedule(
        kwargs["templates_dict"]["retraining_day"],
        day=day,
        repo_name=kwargs["templates_dict"]["repo_name"],
        ds_nodash=kwargs["templates_dict"]["ds_nodash"],
    ):
        return ["retraining_pipeline"]
    else:
        return ["inference_pipeline"]


with airflow.DAG(
    "0001_propensity-p2p",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * 1",
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
            "retraining_day": "monthly",
            "repo_name": "propensity-p2p",
        },
    )

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command='gcloud beta run jobs create propensity-p2p-data-ingestion-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propensity-p2p \
            --tasks 1 \
                --set-env-vars PROCESS="ingesta_datos" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propensity-p2p" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute propensity-p2p-data-ingestion-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    data_validation = BashOperator(
        task_id="data_validation",
        bash_command='gcloud beta run jobs create propensity-p2p-data-validation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propensity-p2p \
            --tasks 1 \
            --set-env-vars PROCESS="validacion_data" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propensity-p2p" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                && gcloud beta run jobs execute propensity-p2p-data-validation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    data_preparation = BashOperator(
        task_id="data_preparation",
        bash_command='gcloud beta run jobs create propensity-p2p-data-preparation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propensity-p2p \
            --tasks 1 \
            --set-env-vars PROCESS="preparacion" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propensity-p2p" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                && gcloud beta run jobs execute propensity-p2p-data-preparation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    model_training = BashOperator(
        task_id="model_training",
        bash_command='gcloud beta run jobs create propensity-p2p-model-training-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propensity-p2p \
            --tasks 1 \
            --set-env-vars PROCESS="entrenamiento" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propensity-p2p" \
                                    --region us-east1 --memory=8192M  --cpu=4 --task-timeout=3600 \
                && gcloud beta run jobs execute propensity-p2p-model-training-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    model_validation = BashOperator(
        task_id="model_validation",
        bash_command='gcloud beta run jobs create propensity-p2p-model-validation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propensity-p2p \
            --tasks 1 \
            --set-env-vars PROCESS="validacion_modelo" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propensity-p2p" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                && gcloud beta run jobs execute propensity-p2p-model-validation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    data_preparation_inference = BashOperator(
        task_id="data_preparation_inference",
        bash_command='gcloud beta run jobs create propensity-p2p-data-prep-infe-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propensity-p2p \
            --tasks 1 \
            --set-env-vars PROCESS="ingesta_datos_prediction" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propensity-p2p" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                && gcloud beta run jobs execute propensity-p2p-data-prep-infe-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference = BashOperator(
        task_id="inference",
        bash_command='gcloud beta run jobs create propensity-p2p-inference-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/propensity-p2p \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="propensity-p2p" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                && gcloud beta run jobs execute propensity-p2p-inference-{{jobs_datetime_name}} \
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
    join >> data_preparation_inference >> inference
