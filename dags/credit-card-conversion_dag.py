from datetime import timedelta, datetime
import pendulum
import os
from google.cloud import storage

import airflow
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from plugins.slack import get_task_success_slack_alert_callback


environment = Variable.get("environment")
project_target = f"tenpo-datalake-{environment}"
slack_conn_id = f"slack_conn-{environment}-models"

# Comando cre crea un vertex ai job asociado a un step de un pipeline
# Primero crea un archivo de configuracion .yaml con las especificaciones
# del job, luego lo ejecuta, y corre un bucle que consulta el estado del mismo
# hasta su finalizacion.
base_command = """
                repo_name={0}
                project_name={1}
                gcp_project_name={2}
                machine_type={3}
                replica_count={4}
                enviroment={5}
                pipeline={6}
                step={7}
                ds_nodash={8}
                ds={9}

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
                echo "        value: '$repo_name'" >> config.yaml
                echo "      - name: PIPELINE" >> config.yaml
                echo "        value: '$pipeline'" >> config.yaml
                echo "      - name: STEP" >> config.yaml
                echo "        value: '$step'" >> config.yaml
                echo "      - name: DS_NODASH" >> config.yaml
                echo "        value: '$ds_nodash'" >> config.yaml
                echo "      - name: DS" >> config.yaml
                echo "        value: '$ds'" >> config.yaml
                
                job_name="$project_name-$(date +%Y%m%d%H%M%S)"

                gcloud ai custom-jobs create \
                --region=us-east1 \
                --display-name="$job_name" \
                --config="config.yaml" \
                --project=$gcp_project_name

                create_job_output=$?

                if [ $create_job_output -eq 0 ]; then
                    echo "Job create OK."
                else
                    echo "Job create error."
                    exit 1
                fi

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
                
                # Ruta vertex (investigar como poder ponerla en un mensaje de slack si el proceso falla)
                location=$(echo "$job_id" | cut -d'/' -f4)
                job_id_vertex=$(echo "$job_id" | cut -d'/' -f6)
                url_vertex="https://console.cloud.google.com/vertex-ai/locations/$location/jobs/$job_id_vertex"
                echo "location = '$location'"
                echo "job_id_vertex = '$job_id_vertex'"
                echo "url_vertex = '$url_vertex'"
            """

default_args = {
    "owner": "Jarvis",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 6, 1, tz="UTC"),
    "email": ["jarvis@tenpo.cl"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # "on_success_callback": get_task_success_slack_alert_callback(slack_conn_id),
    "on_failure_callback": get_task_success_slack_alert_callback(slack_conn_id),
}


# Obtener el día del lunes del mes
def find_first_monday(year, month, day):
    d = datetime(year, int(month), 7)
    offset = -d.weekday()  # weekday = 0 means monday
    return d + timedelta(offset)

def check_exist_model(repo_name: str):
    path_models = f"{repo_name}/models"
    bucket_name = f"mlops-batch-pipelines-{environment}"

    storage_client = storage.Client(project=project_target)
    bucket = storage_client.bucket(bucket_name)

    if list(bucket.list_blobs(prefix=path_models)):
        # Existe un modelo en bucket
        print('Existe un modelo en bucket')
        return False
    else:
        # No existe un modelo en bucket, se requiere entrenamiento
        print('No existe un modelo en bucket, se requiere entrenamiento')
        return True

# Funcion que verifica si en el dia actual hay que ejecutar el
# pipeline de monitoreo, recordar que esto se define en deploy_setup.yaml
def check_monitoring_schedule(monitoring_day, ds_nodash):
    dt = datetime.strptime(ds_nodash, "%Y%m%d")
    date = datetime.strptime(ds_nodash, "%Y%m%d")
    day = str(date.day)

    if monitoring_day == "monthly":
        # Checkear el monitoring si es el primer lunes del mes
        check = date == find_first_monday(dt.year, dt.month, dt.day)
    elif monitoring_day == "daily":
        # Checkear el monitoring todos los días
        check = True
    else:
        check = monitoring_day == day
    return check

# Funcion que verifica el resultado del pipeline del monitoreo,
# cargando el archivo .txt que dejo dicho pipeline cuando se ejecuto.
def check_monitoring_result(bucket_name, folder_name, file_name):
    # Configurar el cliente de Google Cloud Storage
    client = storage.Client(project=project_target)

    # Obtener el blob (archivo) desde el bucket
    bucket = client.bucket(bucket_name)
    file_blob = bucket.blob(f"{folder_name}/{file_name}.txt")

    # Descargar el contenido del archivo como bytes
    file_content = file_blob.download_as_text()

    # Convertir el contenido en una variable booleana
    value = file_content.strip().lower() == "true"

    return value

# Funcion que nos permite decidir si entrenar o no, de
# acuerdo al resultado demonitoring pipeline.
def decide_train(**kwargs):
    ti = kwargs["ti"]
    result = ti.xcom_pull(task_ids="monitoring_result")
    if result:
        return ["training_model"]
    else:
        return ["inference_pipeline"]

# Funcion para branchear entre monitoring pipeline o inference pipeline.
def choose_branch(**kwargs):
    ds_nodash = kwargs["templates_dict"]["ds_nodash"]

    if check_exist_model(repo_name=kwargs["templates_dict"]["repo_name"]):
        return ["training_pipeline"]
    elif check_monitoring_schedule(kwargs["templates_dict"]["monitoring_day"], ds_nodash):
        return ["training_pipeline"]
    else:
        return ["inference_pipeline"]

def choose_branch_monitoring(**kwargs):
    ds_nodash = kwargs["templates_dict"]["ds_nodash"]

    if check_exist_model(repo_name=kwargs["templates_dict"]["repo_name"]):
        print(f'rama elegida training_model')
        return ["training_model"]
    elif check_monitoring_schedule(kwargs["templates_dict"]["monitoring_day"], ds_nodash):
        print(f'rama elegida monitoring_pipeline')
        return ["monitoring_pipeline"]
    else:
        print(f'rama elegida training_model')
        return ["training_model"]


with airflow.DAG(
    "0001_credit-card-conversion",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 4 * * 1",
    max_active_runs=1,
    user_defined_macros={"project_target": project_target},
) as dag:
    # Task asociada al branching.
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        templates_dict={
            "ds_nodash": "{{ds_nodash}}",
            "monitoring_day": "daily",
            "repo_name": "credit-card-conversion",
        },
        
    )
    branching_monitoring = BranchPythonOperator(
        task_id="branching_monitoring",
        python_callable=choose_branch_monitoring,
        templates_dict={
            "ds_nodash": "{{ds_nodash}}",
            "monitoring_day": "daily",
            "repo_name": "credit-card-conversion",
        },
    )
    # Task que ejecuta el monitoring pipeline.
    monitoring = BashOperator(
        task_id="monitoring",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-monitoring",
            project_target,
            "n1-standard-8",
            "1",
            environment,
            "monitoring",
            "monitoring",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
    )
    # Task que chequea el archivo que dejo el monitoring pipeline
    monitoring_result = PythonOperator(
        task_id="monitoring_result",
        provide_context=True,
        python_callable=check_monitoring_result,
        op_args=[
            f"mlops-batch-pipelines-{environment}",
            "credit-card-conversion" + "/monitoring",
            "{{ds}}",
        ],
    )
    # Task que decide de acuerdo a la task anterior, si se ejecuta o no un training pipeline.
    decide_train_task = BranchPythonOperator(
        task_id="decide_train_task", python_callable=decide_train, provide_context=True
    )
    # Task que ejecuta la ingesta de datos del train pipeline.
    train_data_ingestion = BashOperator(
        task_id="train_data_ingestion",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-train-data-ingestion",
            project_target,
            "n1-standard-32",
            "1",
            environment,
            "train",
            "data_ingestion",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
    )
    # Task que ejecuta la validacion de datos del train pipeline.
    train_data_validation = BashOperator(
        task_id="train_data_validation",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-train-data-validation",
            project_target,
            "e2-highmem-16",
            "1",
            environment,
            "train",
            "data_validation",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
    )
    # Task que ejecuta el proprocesamiento de datos del train pipeline.
    train_data_preprocessing = BashOperator(
        task_id="train_data_preprocessing",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-train-data-preprocessing",
            project_target,
            "e2-standard-32",
            "1",
            environment,
            "train",
            "data_preprocessing",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
    )
    # Task que ejecuta el entrenamiento del train pipeline.
    train_train = BashOperator(
        task_id="train_train",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-train-train",
            project_target,
            "n1-standard-32",
            "1",
            environment,
            "train",
            "train",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
    )
    # Task que ejecuta la validacion del modelo del train pipeline.
    train_model_validate = BashOperator(
        task_id="train_model_validate",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-train-model-validate",
            project_target,
            "e2-standard-4",
            "1",
            environment,
            "train",
            "model_validate",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
    )
    # Task que ejecuta la ingesta de datos del inference pipeline.
    inference_data_ingestion = BashOperator(
        task_id="inference_data_ingestion",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-inference-data-ingestion",
            project_target,
            "e2-highmem-16",
            "1",
            environment,
            "inference",
            "data_ingestion",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
        do_xcom_push = True
    )
    # Task que ejecuta la validacion de datos del inference pipeline.
    inference_data_validation = BashOperator(
        task_id="inference_data_validation",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-inference-data-validation",
            project_target,
            "e2-highmem-16",
            "1",
            environment,
            "inference",
            "data_validation",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
        do_xcom_push = True
    )
    # Task que ejecuta el preprocesamiento de datos del inference pipeline.
    inference_data_preprocessing = BashOperator(
        task_id="inference_data_preprocessing",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-inference-data-preprocessing",
            project_target,
            "e2-highmem-16",
            "1",
            environment,
            "inference",
            "data_preprocessing",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
        do_xcom_push = True
    )
    # Task que ejecuta la inferencia.
    inference_inference = BashOperator(
        task_id="inference_inference",
        bash_command=base_command.format(
            "credit-card-conversion",
            "credit-card-conversion-inference-inference",
            project_target,
            "n1-highmem-32",
            "1",
            environment,
            "inference",
            "inference",
            "{{ds_nodash}}",
            "{{ds}}",
        ),
        do_xcom_push = True
    )

    # Tasks dummys que nos ayudan a decidir que camino tomar.
    monitoring_pipeline = DummyOperator(
        task_id="monitoring_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    training_pipeline = DummyOperator(
        task_id="training_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    training_model = DummyOperator(
        task_id="training_model",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    inference_pipeline = DummyOperator(
        task_id="inference_pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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
        >> branching_monitoring
        >> training_model
        >> train_train
        >> train_model_validate
        >> join
    )

    (
        branching_monitoring 
        >> monitoring_pipeline
        >> monitoring
        >> monitoring_result
        >> decide_train_task
        >> training_model
        >> train_train
        >> train_model_validate
        >> join
    )

    (
        decide_train_task
        >> inference_pipeline 
        >> join
    )
    
    (
        branching 
        >> inference_pipeline 
        >> join
    )
    
    (
        join 
        >> inference_data_ingestion
        >> inference_data_validation
        >> inference_data_preprocessing
        >> inference_inference
    )
    