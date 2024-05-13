import os
from copy import deepcopy
from datetime import timedelta
import pendulum
import yaml
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from plugins.cloud_run_plugin import CloudRunOperator
from plugins.dataproc_serverless_plugin import DataprocServerlessOperator
from plugins.slack import get_task_success_slack_alert_callback
from airflow.utils.dates import days_ago
from airflow.models import Variable

environment = Variable.get("environment")
SLACK_CONN_ID = f"slack_conn-{environment}"
YAMLS_PATH = "/opt/airflow/dags/repo/dags/repo/dags/batch_dags/pipe"

class DagFactory:
    DEFAULT_DAG_ARGS = {
        "owner": "Jarvis",
        "depends_on_past": False,
        "start_date":days_ago(1),
        "email": ["jarvis@tenpo.cl"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "provide_context": True,
        "on_failure_callback": get_task_success_slack_alert_callback(SLACK_CONN_ID)
    }

    def run(self):
        dags_generator = self._create_dags(YAMLS_PATH)
        for dag in dags_generator:
            globals()[dag.dag_id] = dag

    def _create_dags(self, yamls_source_path):
        data_sources = [
            os.path.join(yamls_source_path, p) for p in os.listdir(yamls_source_path)
        ]
        for data_source in data_sources:
            json_data = self._parse_yaml(data_source)
            yield self._create_dag_etl(json_data)

    def _parse_yaml(self, data_source):
        with open(data_source, "r") as f:
            return yaml.safe_load(f)

    def _create_dag_etl(self, json_data):
        new_default_args = json_data.get("default_args", {})
        dag_args = json_data["dag_args"]
        dag_type = json_data["type"]
        tasks = json_data["tasks"]
        dag = self._create_dag(new_default_args, dag_type, dag_args)
        if dag_type == "cloudrun":
            self._create_cloudrun_tasks(dag, len(tasks), tasks)
        elif dag_type == "dataproc":
            self._create_dataproc_tasks(dag, tasks)
        else:
            raise ValueError("Unknown dag_type: {}".format(dag_type))

        return dag

    def _create_dag(self, default_args, dag_type, dag_args):
        new_default_args = deepcopy(DagFactory.DEFAULT_DAG_ARGS)
        new_default_args.update(default_args)
        return DAG(default_args=new_default_args, catchup=False, tags=["batch", dag_type], **dag_args)

    def _create_dataproc_tasks(self, dag, tasks:list):
        with dag:
            dummy_task = DummyOperator(task_id="initial_task", dag=dag)
            """Only supports one batch task"""
            python_task = DataprocServerlessOperator(
                task_id="launch_dataproc_serverless", task_args=tasks[0]['task_data']
            )

            dummy_task >> python_task

    def _create_cloudrun_tasks(self, dag, number_of_tasks: int, tasks: list):
        with dag:
            dummy_task = DummyOperator(task_id="initial_task", dag=dag)
            
            if number_of_tasks == 1:
                python_task_1 = CloudRunOperator(task_id="request_cloud_run_1", task_args= tasks[0]['task_data'])
                
                dummy_task >> python_task_1

            elif number_of_tasks == 2:
                python_task_1 = CloudRunOperator(task_id="request_cloud_run_1", task_args= tasks[0]['task_data'])
                python_task_2 = CloudRunOperator(task_id="request_cloud_run_2", task_args= tasks[1]['task_data'])

                dummy_task >> python_task_1 >> python_task_2
            
            elif number_of_tasks == 3:
                python_task_1 = CloudRunOperator(task_id="request_cloud_run_1", task_args= tasks[0]['task_data'])
                python_task_2 = CloudRunOperator(task_id="request_cloud_run_2", task_args= tasks[1]['task_data'])
                python_task_3 = CloudRunOperator(task_id="request_cloud_run_3", task_args= tasks[2]['task_data'])

                dummy_task >> python_task_1 >> python_task_2 >> python_task_3
            
            elif number_of_tasks == 4:
                python_task_1 = CloudRunOperator(task_id="request_cloud_run_1", task_args= tasks[0]['task_data'])
                python_task_2 = CloudRunOperator(task_id="request_cloud_run_2", task_args= tasks[1]['task_data'])
                python_task_3 = CloudRunOperator(task_id="request_cloud_run_3", task_args= tasks[2]['task_data'])
                python_task_4 = CloudRunOperator(task_id="request_cloud_run_4", task_args= tasks[3]['task_data'])

                dummy_task >> python_task_1 >> python_task_2 >> python_task_3 >> python_task_4
            
            elif number_of_tasks == 5:
                python_task_1 = CloudRunOperator(task_id="request_cloud_run_1", task_args= tasks[0]['task_data'])
                python_task_2 = CloudRunOperator(task_id="request_cloud_run_2", task_args= tasks[1]['task_data'])
                python_task_3 = CloudRunOperator(task_id="request_cloud_run_3", task_args= tasks[2]['task_data'])
                python_task_4 = CloudRunOperator(task_id="request_cloud_run_4", task_args= tasks[3]['task_data'])
                python_task_5 = CloudRunOperator(task_id="request_cloud_run_5", task_args= tasks[4]['task_data'])

                dummy_task >> python_task_1 >> python_task_2 >> python_task_3 >> python_task_4 >> python_task_5
            
            else:
                raise ValueError("Too many tasks: Only 5 tasks allowed")
            

DagFactory().run()
