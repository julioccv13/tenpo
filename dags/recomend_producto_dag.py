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
    path_models = f"models/{repo_name}/saved_models/current_model/modelo/saved_model.pb"
    bucket_name = f"jarvis_mlops_{environment}"
    storage_client = storage.Client(project=project_target)
    bucket = storage_client.bucket(bucket_name)

    dt = datetime.strptime(ds_nodash, "%Y%m%d")
    date = datetime.strptime(ds_nodash, "%Y%m%d")

    # Entrenamiento el primer lunes de cada mes
    if retraining_day == "monthly":
        check = (date == find_first_monday(dt.year, dt.month, dt.day)) | (
            (storage.Blob(bucket=bucket, name=path_models).exists()) == False
        )
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
    "0001_recomend-producto",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 2 * * 1",
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
            "repo_name": "recomend-producto",
        },
    )

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command='gcloud beta run jobs create recomend-producto-data-ingestion-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
                --set-env-vars PROCESS="ingesta_datos" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=8192M  --cpu=4 --task-timeout=3600 \
                    && gcloud beta run jobs execute recomend-producto-data-ingestion-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    data_preparation = BashOperator(
        task_id="data_preparation",
        bash_command='gcloud beta run jobs create recomend-producto-data-preparation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="preparacion" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-data-preparation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    model_training = BashOperator(
        task_id="model_training",
        bash_command='gcloud beta run jobs create recomend-producto-model-training-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="entrenamiento" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-model-training-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    model_validation = BashOperator(
        task_id="model_validation",
        bash_command='gcloud beta run jobs create recomend-producto-model-validation-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="validacion_modelo" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-model-validation-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_mastercard = BashOperator(
        task_id="inference_mastercard",
        bash_command='gcloud beta run jobs create recomend-producto-inference-mastercard-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="mastercard" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-mastercard-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_mastercard_p = BashOperator(
        task_id="inference_mastercard_p",
        bash_command='gcloud beta run jobs create recomend-producto-inference-mastercard-p-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="mastercard_physical" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-mastercard-p-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_p2p = BashOperator(
        task_id="inference_p2p",
        bash_command='gcloud beta run jobs create recomend-producto-inference-p2p-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="p2p" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-p2p-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_p2p_received = BashOperator(
        task_id="inference_p2p_received",
        bash_command='gcloud beta run jobs create recomend-producto-inference-p2p-received-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="p2p_received" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-p2p-received-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_paypal = BashOperator(
        task_id="inference_paypal",
        bash_command='gcloud beta run jobs create recomend-producto-inference-paypal-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="paypal" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-paypal-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_paypal_abonos = BashOperator(
        task_id="inference_paypal_abonos",
        bash_command='gcloud beta run jobs create recomend-producto-inference-paypal-abonos-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="paypal_abonos" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-paypal-abonos-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_top_ups = BashOperator(
        task_id="inference_top_ups",
        bash_command='gcloud beta run jobs create recomend-producto-inference-top-ups-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="top_ups" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-top-ups-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_utility_payments = BashOperator(
        task_id="inference_utility_payments",
        bash_command='gcloud beta run jobs create recomend-producto-inference-utility-payments-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="utility_payments" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-utility-payments-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_crossborder = BashOperator(
        task_id="inference_crossborder",
        bash_command='gcloud beta run jobs create recomend-producto-inference-crossborder-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="crossborder" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-crossborder-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_savings = BashOperator(
        task_id="inference_savings",
        bash_command='gcloud beta run jobs create recomend-producto-inference-savings-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="cash_in_savings" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-savings-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    inference_tyba = BashOperator(
        task_id="inference_tyba",
        bash_command='gcloud beta run jobs create recomend-producto-inference-tyba-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="inferencia" \
                --set-env-vars LINEA="investment_tyba" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=32768M  --cpu=8 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-inference-tyba-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    join_table = BashOperator(
        task_id="join_table",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        bash_command='gcloud beta run jobs create recomend-producto-join-table-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/recomend-producto \
            --tasks 1 \
            --set-env-vars PROCESS="join_table" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="recomend-producto" \
                                    --region us-east1 --memory=8192M  --cpu=4 --task-timeout=3600 \
                && gcloud beta run jobs execute recomend-producto-join-table-{{jobs_datetime_name}} \
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
        data_ingestion
        >> branching
        >> retraining_pipeline
        >> data_preparation
        >> model_training
        >> model_validation
        >> join
    )
    (data_ingestion >> branching >> inference_pipeline >> join)
    (
        join
        >> [
            inference_mastercard,
            inference_mastercard_p,
            inference_p2p,
            inference_p2p_received,
            inference_paypal,
            inference_paypal_abonos,
            inference_top_ups,
            inference_utility_payments,
            inference_crossborder,
            inference_savings,
            inference_tyba,
        ]
        >> join_table
    )
