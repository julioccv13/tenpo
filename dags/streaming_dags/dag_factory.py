import os
from datetime import datetime, timedelta

import pendulum
import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.kubernetes.secret import Secret

from plugins.gke_start_pod_templated_plugin import GKEStartPodTemplatedOperator


class DagFactory:
    NAMESPACE = "streaming"
    SERVICE_ACCOUNT_NAME = Variable.get(
        "streaming_service_account_name", "streaming-sa"
    )
    PIPELINES_PATH = "/opt/airflow/dags/repo/dags/repo/dags/streaming_dags/pipe"

    local_tz = pendulum.timezone("America/Santiago")
    start_date = datetime(2022, 11, 17, tzinfo=local_tz)
    schedule_interval = "0 0 * * *"
    environment = Variable.get("environment")
    cluster_name = f"{environment}-k8s-cluster"
    project_id = f"tenpo-datalake-{environment}"
    location = "us-east4"
    default_args = {
        "start_date": start_date,
        "startup_timeout_seconds": 300,
        "retries": 2,
        "retry_delay": 0,
        "namespace": NAMESPACE,
        "service_account_name": SERVICE_ACCOUNT_NAME,
        "depends_on_past": False,
        "owner": "d.pineda.externo@tenpo.cl",
        "location": location,
        "cluster_name": cluster_name,
        "project_id": project_id,
        "in_cluster": False,
        "is_delete_operator_pod": True,
        "execution_timeout": timedelta(seconds=86400),
    }

    def run(self) -> None:
        pipelines = [
            os.path.join(self.PIPELINES_PATH, p)
            for p in os.listdir(self.PIPELINES_PATH)
        ]
        for pipeline in pipelines:
            dag_generator = self._create_dag_etl(pipeline)
            for dag in dag_generator:
                globals()[dag.dag_id] = dag

    def _create_dag_etl(self, data_source_path: str):
        pipeline = data_source_path.split("/")[-1].split(".")[0]
        tables = self.get_yaml_data(data_source_path)
        for table, config in tables.items():
            dag_id = f"{pipeline}_{table}"
            dag = self._create_dag(dag_id)
            self._create_task(dag, config)

            yield dag

    def _create_dag(self, dag_id: str) -> DAG:
        dag = DAG(
            dag_id,
            default_args=self.default_args,
            description="",
            schedule_interval=self.schedule_interval,
            catchup=False,
            max_active_runs=1,
            tags=["streaming"],
        )
        return dag

    def _create_task(self, dag: DAG, config: dict):
        secrets = config.get("secrets")
        if secrets:
            secrets = self._parse_secrets(secrets, config["secret_name"])
        task = GKEStartPodTemplatedOperator(
            task_id="run",
            location=self.location,
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            name=config["name"],
            namespace=config.get("namespace", self.NAMESPACE),
            image=config["docker_image"],
            arguments=config["arguments"],
            dag=dag,
            do_xcom_push=True,
            secrets=secrets,
            env_vars=config.get("env_vars", {}),
            get_logs=True,
            log_events_on_failure=True,
            is_delete_operator_pod=True,
            image_pull_policy="Always"
        )
        return task

    @staticmethod
    def _parse_secrets(secrets: dict, secret: str) -> list:
        return [
            Secret(
                deploy_type="env",
                deploy_target=secret_name,
                secret=secret,
                key=secret_key,
            )
            for secret_name, secret_key in secrets.items()
        ]

    @staticmethod
    def get_yaml_data(filename: str) -> dict:
        with open(filename, "r") as f:
            return yaml.safe_load(f)


DagFactory().run()
