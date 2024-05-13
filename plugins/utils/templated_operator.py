from typing import Any

import yaml
from airflow.models import BaseOperator
from jinja2 import Template


class TemplatedOperator(BaseOperator):
    def __init__(self, task_args, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_args = task_args

    def pre_execute(self, context: Any):
        self.task_args = self._deep_render_template(self.task_args, context)

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

    def _render_template(self, string_template, context):
        tmp = Template(string_template)
        data = tmp.render(**context)
        return str(yaml.safe_load(data))
