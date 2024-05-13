from datetime import timedelta, datetime
import pendulum

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
            """

# Funcion que verifica si en el dia actual hay que ejecutar el 
# pipeline de monitoreo, recordar que esto se define en deploy_setup.yaml
def check_monitoring_schedule(monitoring_day, day):

    if monitoring_day == "monthly":
        check = (day == "25")
    elif monitoring_day == "daily":
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
    file_blob = bucket.blob(f"{folder_name}/{file_name}")

    # Descargar el contenido del archivo como bytes
    file_content = file_blob.download_as_text()

    # Convertir el contenido en una variable booleana
    value = file_content.strip().lower() == "true"

    return value

# Funcion que nos permite decidir si entrenar o no, de 
# acuerdo al resultado demonitoring pipeline.
def decide_train(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids="monitoring_result")
    if result:
        return ["training_pipeline"]
    else:
        return ["inference_pipeline"]

# Funcion para branchear entre monitoring pipeline o inference pipeline.
def choose_branch(**kwargs):
    date = datetime.strptime(kwargs["templates_dict"]["ds_nodash"], "%Y%m%d")
    day = str(date.day)

    if check_monitoring_schedule(
        kwargs["templates_dict"]["monitoring_day"],
        day
    ):
        return ["monitoring_pipeline"]
    else:
        return ["inference_pipeline"]


with airflow.DAG(
    "0001_hook-optim-m1-paypal-sinhook",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    user_defined_macros={
        "project_target": project_target
    },
) as dag:
    
    # Task asociada al branching.
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
        templates_dict={
            "ds_nodash": "{{ds_nodash}}",
            "monitoring_day": "monthly"
        },
    )
    # Task que ejecuta el monitoring pipeline.
    monitoring = BashOperator(
        task_id="monitoring",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-monitoring",
                                            project_target,
                                            "e2-standard-8",
                                            "1",
                                            environment,
                                            "monitoring",
                                            "monitoring",
                                            "{{ds_nodash}}")
    )
    # Task que chequea el archivo que dejo el monitoring pipeline
    monitoring_result = PythonOperator(
        task_id="monitoring_result",
        provide_context=True,
        python_callable=check_monitoring_result,
        op_args=[f"mlops-batch-pipelines-{environment}",
                  "hook-optim-m1-paypal-sinhook" + "/monitoring",
                   f"{str(datetime.today().strftime('%Y-%m-%d'))}.txt" ]
    )
    # Task que decide de acuerdo a la task anterior, si se ejecuta o no un training pipeline.
    decide_train_task = BranchPythonOperator(
        task_id="decide_train_task",
        python_callable=decide_train,
        provide_context=True
    )
    # Task que ejecuta la ingesta de datos del train pipeline.
    train_data_ingestion = BashOperator(
        task_id="train_data_ingestion",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-train-data-ingestion",
                                            project_target,
                                            "e2-standard-8",
                                            "1",
                                            environment,
                                            "train",
                                            "data_ingestion",
                                            "{{ds_nodash}}")
    )
    # Task que ejecuta la validacion de datos del train pipeline.
    train_data_validation = BashOperator(
        task_id="train_data_validation",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-train-data-validation",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "train",
                                           "data_validation",
                                           "{{ds_nodash}}")
    )
    # Task que ejecuta el proprocesamiento de datos del train pipeline.
    train_data_preprocessing = BashOperator(
        task_id="train_data_preprocessing",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-train-data-preprocessing",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "train",
                                           "data_preprocessing",
                                           "{{ds_nodash}}")
    )
    # Task que ejecuta el entrenamiento del train pipeline.
    train_train = BashOperator(
        task_id="train_train",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-train-train",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "train",
                                           "train",
                                           "{{ds_nodash}}")
    )
    # Task que ejecuta la validacion del modelo del train pipeline.
    train_model_validate = BashOperator(
        task_id="train_model_validate",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-train-model-validate",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "train",
                                           "model_validate",
                                           "{{ds_nodash}}")
    )
    # Task que ejecuta la ingesta de datos del inference pipeline.
    inference_data_ingestion = BashOperator(
        task_id="inference_data_ingestion",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-inference-data-ingestion",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "inference",
                                           "data_ingestion",
                                           "{{ds_nodash}}")
    )
    # Task que ejecuta la validacion de datos del inference pipeline.
    inference_data_validation = BashOperator(
        task_id="inference_data_validation",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-inference-data-validation",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "inference",
                                           "data_validation",
                                           "{{ds_nodash}}")
    )
    # Task que ejecuta el preprocesamiento de datos del inference pipeline.
    inference_data_preprocessing = BashOperator(
        task_id="inference_data_preprocessing",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-inference-data-preprocessing",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "inference",
                                           "data_preprocessing",
                                           "{{ds_nodash}}")
    )
    # Task que ejecuta la inferencia.
    inference_inference = BashOperator(
        task_id="inference_inference",
        bash_command = base_command.format("hook-optim-m1-paypal-sinhook",
                                           "hook-optim-m1-paypal-sinhook-inference-inference",
                                           project_target,
                                           "e2-standard-8",
                                           "1",
                                           environment,
                                           "inference",
                                           "inference",
                                           "{{ds_nodash}}")
    )

    # Tasks dummys que nos ayudan a decidir que camino tomar.
    monitoring_pipeline = DummyOperator(
        task_id="monitoring_pipeline",
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
        inference_data_ingestion
        >> inference_data_validation
        >> inference_data_preprocessing
        >> branching
        >> monitoring_pipeline
        >> monitoring
        >> monitoring_result
        >> decide_train_task
        >> training_pipeline
        >> train_data_ingestion
        >> train_data_validation
        >> train_data_preprocessing
        >> train_train
        >> train_model_validate
        >> join
    )
    branching >> inference_pipeline >> join
    join >> inference_inference