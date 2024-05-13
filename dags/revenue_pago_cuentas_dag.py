from datetime import timedelta, datetime
from typing import Dict
import pendulum

import os

os.system("pip install PyYAML")
import yaml

from google.cloud import storage

import airflow
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator

environment = Variable.get("environment")
project_target = f"tenpo-datalake-{environment}"

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


def check_retraing_schedule(retraining_day, day, repo_name):
    path_models = f"models/{repo_name}/current_model/modelo.joblib"
    bucket_name = f"jarvis_mlops_{environment}"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # check if execution day equal 1
    if retraining_day == "monthly":
        check = (day == "1") | ((storage.Blob(bucket=bucket, name=path_models).exists()) == False)
    # if is daily always true
    elif retraining_day == "daily":
        print("inferencia")
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
    ):
        return ["retraining_pipeline"]
    else:
        return ["inference_pipeline"]


base_command = """
                project_target={0}
                ds={1}
                ds_nodash={2}
                repo_name={3}
                machine_type={4}
                replica_count={5}
                process={6}
                process_job_name={7}
                

                echo "workerPoolSpecs:" > config.yaml
                echo "  machineSpec:" >> config.yaml
                echo "    machine_type: $machine_type" >> config.yaml
                echo "  replicaCount: $replica_count" >> config.yaml
                echo "  containerSpec:" >> config.yaml
                echo "    imageUri: gcr.io/$project_target/$repo_name" >> config.yaml
                echo "    env:" >> config.yaml
                echo "      - name: PROCESS" >> config.yaml
                echo "        value: '$process'" >> config.yaml
                echo "      - name: DS_NODASH" >> config.yaml
                echo "        value: '$ds_nodash'" >> config.yaml
                echo "      - name: DS" >> config.yaml
                echo "        value: '$ds'" >> config.yaml
                echo "      - name: PROJECT_TARGET" >> config.yaml
                echo "        value: '$project_target'" >> config.yaml
                echo "      - name: REPO_NAME" >> config.yaml
                echo "        value: '$repo_name'" >> config.yaml

                job_name="$repo_name-$process_job_name-$(date +%Y%m%d%H%M%S)"

                gcloud ai custom-jobs create \
                --region=us-east1 \
                --display-name="$job_name" \
                --config="config.yaml" \
                --project=$project_target

                job_id=$(gcloud ai custom-jobs list \
                    --format="value(name)" \
                    --filter="displayName:$job_name" \
                    --region=us-east1 \
                    --project="$project_target" \
                    --limit=1)

                while true; do
                    STATE=$(gcloud ai custom-jobs describe "$job_id" --format='value(state)')
                    echo $STATE
                    if [[ $STATE == "JOB_STATE_SUCCEEDED" ]]; then
                    echo "SUCCEEDED JOB."
                        break
                    elif [[ $STATE == "JOB_STATE_FAILED" ]]; then
                        echo "FAILED JOB."
                        exit 1
                    fi
                    sleep 10
                done
            """

with airflow.DAG(
    "0001_revenue-pago-cuentas",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    user_defined_macros={"project_target": project_target},
) as dag:
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        templates_dict={
            "ds_nodash": "{{ds_nodash}}",
            "retraining_day": "monthly",
            "repo_name": "revenue-pago-cuentas",
        },
    )

    data_ingestion = BashOperator(
        task_id="data_ingestion",
        bash_command=base_command.format(
            project_target,
            "{{ds}}",
            "{{ds_nodash}}",
            "revenue-pago-cuentas",
            "n1-standard-4",
            1,
            "data_ingestion",
            "data-ingestion",
        ),
    )

    data_validation = BashOperator(
        task_id="data_validation",
        bash_command=base_command.format(
            project_target,
            "{{ds}}",
            "{{ds_nodash}}",
            "revenue-pago-cuentas",
            "n1-standard-4",
            1,
            "data_validation",
            "data-validation",
        ),
    )

    data_preparation = BashOperator(
        task_id="data_preparation",
        bash_command=base_command.format(
            project_target,
            "{{ds}}",
            "{{ds_nodash}}",
            "revenue-pago-cuentas",
            "n1-standard-4",
            1,
            "data_preparation",
            "data-preparation",
        ),
    )

    model_training = BashOperator(
        task_id="model_training",
        bash_command=base_command.format(
            project_target,
            "{{ds}}",
            "{{ds_nodash}}",
            "revenue-pago-cuentas",
            "n1-standard-4",
            1,
            "model_training",
            "model-training",
        ),
    )

    model_validation = BashOperator(
        task_id="model_validation",
        bash_command=base_command.format(
            project_target,
            "{{ds}}",
            "{{ds_nodash}}",
            "revenue-pago-cuentas",
            "n1-standard-4",
            1,
            "model_validation",
            "model-validation",
        ),
    )

    data_preparation_inference = BashOperator(
        task_id="data_preparation_inference",
        bash_command=base_command.format(
            project_target,
            "{{ds}}",
            "{{ds_nodash}}",
            "revenue-pago-cuentas",
            "n1-standard-4",
            1,
            "data_preparation_inference",
            "data-prep-infe",
        ),
    )

    inference = BashOperator(
        task_id="inference",
        bash_command=base_command.format(
            project_target,
            "{{ds}}",
            "{{ds_nodash}}",
            "revenue-pago-cuentas",
            "n1-standard-4",
            1,
            "inference",
            "inference",
        ),
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
