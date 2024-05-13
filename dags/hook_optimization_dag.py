from datetime import timedelta, datetime
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
    "start_date": pendulum.datetime(2023, 6, 1, tz="UTC"),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

base_command = """
                repo_name={0}
                project_name={1}
                gcp_project_name={2}
                machine_type={3}
                replica_count={4}
                enviroment={5}
                pipeline={6}
                step={7}

                echo "workerPoolSpecs:" > config.yaml
                echo "  machineSpec:" >> config.yaml
                echo "    machine_type: $machine_type" >> config.yaml
                echo "  replicaCount: $replica_count" >> config.yaml
                echo "  containerSpec:" >> config.yaml
                echo "    imageUri: gcr.io/$gcp_project_name/$repo_name:latest" >> config.yaml
                echo "    env:" >> config.yaml
                echo "      - name: ENVIROMENT" >> config.yaml
                echo "        value: '$enviroment'" >> config.yaml
                echo "      - name: PROJECT_NAME" >> config.yaml
                echo "        value: '$gcp_project_name'" >> config.yaml
                echo "      - name: PIPELINE" >> config.yaml
                echo "        value: '$pipeline'" >> config.yaml
                echo "      - name: STEP" >> config.yaml
                echo "        value: '$step'" >> config.yaml

                job_name="$project_name-$(date +%Y%m%d%H%M%S)"

                gcloud ai custom-jobs create \
                --region=us-east1 \
                --display-name="$job_name" \
                --config="config.yaml" \
                --project=$gcp_project_name

                job_id=$(gcloud ai custom-jobs list \
                    --format="value(name)" \
                    --filter="displayName:$job_name" \
                    --region=us-east1 \
                    --project="$gcp_project_name" \
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

def check_train_schedule(training_day, day):

    if training_day == "monthly":
        check = (day == "1")
    elif training_day == "daily":
        check = True
    else:
        check = training_day == day
    return check


def choose_branch(**kwargs):
    date = datetime.strptime(kwargs["templates_dict"]["ds_nodash"], "%Y%m%d")
    day = str(date.day)

    if check_train_schedule(
        kwargs["templates_dict"]["retraining_day"],
        day
    ):
        return ["training_pipeline"]
    else:
        return ["inference_pipeline"]


with airflow.DAG(
    "0001_hook-optimization",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    user_defined_macros={
        "project_target": project_target
    },
) as dag:
    
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        templates_dict={
            "ds_nodash": "{{ds_nodash}}",
            "retraining_day": "daily"
        },
    )

    train_data_ingestion = BashOperator(
        task_id="train_data_ingestion",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-train-data-ingestion",
                                            project_target,
                                            "n1-standard-4",
                                            "1",
                                            environment,
                                            "train",
                                            "data_ingestion")
    )

    train_data_validation = BashOperator(
        task_id="train_data_validation",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-train-data-validation",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "train",
                                           "data_validation")
    )

    train_data_preprocessing = BashOperator(
        task_id="train_data_preprocessing",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-train-data-preprocessing",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "train",
                                           "data_preprocessing")
    )

    train_train = BashOperator(
        task_id="train_train",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-train-train",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "train",
                                           "train")
    )

    train_model_validate = BashOperator(
        task_id="train_model_validate",
        bash_command = base_command.format("hook-optimization-train-model-validate",
                                           "hook-optimization",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "train",
                                           "model_validate")
    )

    inference_data_ingestion = BashOperator(
        task_id="inference_data_ingestion",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-inference-data-ingestion",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "inference",
                                           "data_ingestion")
    )

    inference_data_validation = BashOperator(
        task_id="inference_data_validation",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-inference-data-validation",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "inference",
                                           "data_validation")
    )

    inference_data_preprocessing = BashOperator(
        task_id="inference_data_preprocessing",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-inference-data-preprocessing",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "inference",
                                           "data_preprocessing")
    )

    inference_inference = BashOperator(
        task_id="inference_inference",
        bash_command = base_command.format("hook-optimization",
                                           "hook-optimization-inference-inference",
                                           project_target,
                                           "n1-standard-4",
                                           "1",
                                           environment,
                                           "inference",
                                           "inference")
    )

    training_pipeline = DummyOperator(
        task_id="training_pipeline",
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
        >> training_pipeline
        >> train_data_ingestion
        >> train_data_validation
        >> train_data_preprocessing
        >> train_train
        >> train_model_validate
        >> join
    )
    branching >> inference_pipeline >> join
    join >> inference_data_ingestion >> inference_data_validation >> inference_data_preprocessing >> inference_inference
