import yaml
from jinja2 import Template

from airflow.plugins_manager import AirflowPlugin

from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator
)


class GKEStartPodTemplatedOperator(GKEStartPodOperator):
    def pre_execute(self, context: any):
        task_args = vars(self)
        task_args = self._deep_render_template(task_args, context)
        for key in self.__dict__.keys():
            self.__dict__[key] = task_args[key]

    def _deep_render_template(self, json_data, context):
        data = {}
        for key, value in json_data.items():
            data[key] = self._render_value(value, context)
        return data

    def _render_value(self, value, context):
        if isinstance(value, str):
            return self._render_template(value, context)
        elif isinstance(value, list):
            return [self._render_value(val, context) for val in value]
        elif isinstance(value, dict):
            return self._deep_render_template(value, context)
        return value

    def _render_template(self, string_template, context):
        tmp = Template(string_template)
        data = tmp.render(**context)
        return str(yaml.safe_load(data))


class GKEStartPodTemplatedPlugin(AirflowPlugin):
    name = "Gke Start Pod Templated Plugin"
    operators = [GKEStartPodTemplatedOperator]
