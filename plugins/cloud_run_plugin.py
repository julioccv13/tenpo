import requests
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin

from plugins.utils.templated_operator import TemplatedOperator


class CloudRunOperator(TemplatedOperator):
    def __init__(self, task_args, *args, **kwargs):
        super().__init__(*args, task_args=task_args, **kwargs)

    def execute(self, context):
        cloudrun_url = Variable.get("cloudrun_url")
        response = requests.post(cloudrun_url, json=self.task_args)
        if "Success" in response.text and response.status_code == 200:
            return response.text
        else:
            raise RuntimeError(response.text)


class CloudRunPlugin(AirflowPlugin):
    name = "Cloud Run Plugin"
    operators = [CloudRunOperator]
