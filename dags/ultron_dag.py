from datetime import timedelta, datetime
import pandas as pd
from typing import Dict
import pendulum
import requests

import airflow
from airflow.decorators import dag, task
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

with airflow.DAG(
    "0001_ultron",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    user_defined_macros={
        "project_target": project_target,  # Macro can be a variable
        "jobs_datetime_name": jobs_datetime_name,
    },
) as dag:

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command='gcloud beta run jobs create ultron-data-ingestion-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/ultron \
            --tasks 1 \
                --set-env-vars PROCESS="data_ingestion" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="ultron" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute ultron-data-ingestion-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    score_paypal = BashOperator(
        task_id="score_paypal",
        bash_command='gcloud beta run jobs create ultron-score-paypal-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/ultron \
            --tasks 1 \
                --set-env-vars PROCESS="score_paypal" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="ultron" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute ultron-score-paypal-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    score_investment = BashOperator(
        task_id="score_investment",
        bash_command='gcloud beta run jobs create ultron-score-investment-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/ultron \
            --tasks 1 \
                --set-env-vars PROCESS="score_investment" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="ultron" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute ultron-score-investment-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    score_utility = BashOperator(
        task_id="score_utility",
        bash_command='gcloud beta run jobs create ultron-score-utility-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/ultron \
            --tasks 1 \
                --set-env-vars PROCESS="score_utility" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="ultron" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute ultron-score-utility-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    score_p2p = BashOperator(
        task_id="score_p2p",
        bash_command='gcloud beta run jobs create ultron-score-p2p-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/ultron \
            --tasks 1 \
                --set-env-vars PROCESS="score_p2p" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="ultron" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute ultron-score-p2p-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    score_mastercard = BashOperator(
        task_id="score_mastercard",
        bash_command='gcloud beta run jobs create ultron-score-mastercard-{{jobs_datetime_name}} \
        --image gcr.io/{{ project_target }}/ultron \
            --tasks 1 \
                --set-env-vars PROCESS="score_mastercard" \
                    --set-env-vars DS_NODASH="{{ds_nodash}}" \
                        --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="ultron" \
                                    --region us-east1 --memory=8192M  --cpu=4 \
                    && gcloud beta run jobs execute ultron-score-mastercard-{{jobs_datetime_name}} \
                        --wait --region us-east1',
    )

    join_table = BashOperator(
        task_id="join_table",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        bash_command='gcloud beta run jobs create ultron-join-table-{{ds_nodash}} \
        --image gcr.io/{{ project_target }}/ultron \
            --tasks 1 \
            --set-env-vars PROCESS="join_table" \
                --set-env-vars DS_NODASH="{{ds_nodash}}" \
                    --set-env-vars DS="{{ds}}" \
                            --set-env-vars  PROJECT_TARGET="{{ project_target }}" \
                                --set-env-vars  REPO_NAME="ultron" \
                                    --region us-east1 --memory=8192M  --cpu=4 --task-timeout=3600 \
                && gcloud beta run jobs execute ultron-join-table-{{ds_nodash}} \
                        --wait --region us-east1',
    )

    join = DummyOperator(
        task_id="join",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    delete_old_jobs = BashOperator(
        task_id="delete_old_jobs",
        bash_command='#!/bin/bash\n\
                  project_name=ultron\n\
                  date_7_days_ago=$(date --iso-8601=seconds --date="7 days ago")\n\
                  job_names=($(gcloud beta run jobs list --format="value(metadata.name)" --filter="metadata.name~$project_name AND metadata.creationTimestamp<$date_7_days_ago"))\n\
                  for job_name in $job_names\n\
                  do\n\
                    echo "Deleting job $job_name..."\n\
                    gcloud beta run jobs delete "$job_name" --region us-east1 --quiet\n\
                  done\n\
                  echo -e "All project Cloud Runs jobs are deleted"\n',
    )

(
    delete_old_jobs
    >> data_ingestion
    >> [score_paypal, score_investment, score_utility, score_p2p, score_mastercard]
    >> join
    >> join_table
)

# (
#     data_ingestion
#     >> score_paypal
#     >> score_investment
#     >> score_utility
#     >> score_p2p
#     >> score_mastercard
#     >> delete_old_jobs
# )
#
