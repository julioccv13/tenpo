import logging
from time import sleep

from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.compat.functools import cached_property

from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.hooks.subprocess import SubprocessHook

from google.cloud.dataproc_v1.types import Batch

from plugins.utils.templated_operator import TemplatedOperator
from plugins.utils.google import check_existence, delete_files


BATCH_STATE_ENUM_REFERENCE = {
    Batch.State.STATE_UNSPECIFIED: "STATE_UNSPECIFIED",
    Batch.State.PENDING: "PENDING",
    Batch.State.RUNNING: "RUNNING",
    Batch.State.CANCELLING: "CANCELLING",
    Batch.State.CANCELLED: "CANCELLED",
    Batch.State.SUCCEEDED: "SUCCEEDED",
    Batch.State.FAILED: "FAILED",
}


class DataprocServerlessOperator(TemplatedOperator):
    def __init__(
        self,
        task_args,
        *args,
        **kwargs,
    ):
        super().__init__(*args, task_args=task_args, **kwargs)

    def pre_execute(self, context):
        super().pre_execute(context)
        self.project_id = self.task_args["project_id"]
        self.date_to_process = self.task_args["date_to_process"]
        self.source_bucket = self.task_args["source_bucket"]
        self.target_bucket = self.task_args["target_bucket"]
        self.batch_config = self.task_args["batch_config"]
        self.batch_id = self.task_args["batch_id"]
        self.default_region = self.task_args["default_region"]
        self.logger = logging.getLogger("airflow.task")

    @cached_property
    def dataproc_hook(self):
        return DataprocHook()

    @cached_property
    def subprocess_hook(self):
        return SubprocessHook()

    def execute(self, context):
        self._validate_source_files_existence()
        self._clear_target_files()
        self._list_dataproc_batches()
        self._get_region()
        self._create_batch()
        self._get_batch()
        self._delete_batch()
        self._validate_result()

    def _validate_source_files_existence(self):
        prefix = f"dt={self.date_to_process}/"
        self.logger.info(f"source_bucket: {self.source_bucket}")
        if not check_existence(self.source_bucket, prefix):
            raise AirflowSkipException("No hay archivos para procesar")

    def _clear_target_files(self):
        prefix = f"dt={self.date_to_process}/"
        delete_files(self.target_bucket, prefix)

    def _list_dataproc_batches(self):
        results = list(
            self.dataproc_hook.list_batches(
                region=self.default_region, project_id=self.project_id
            )
        )
        self.logger.info(results)

    def _get_region(self):
        command = """
        sleep 65
        zones="us-east1 us-west1 us-west2 us-central1"
        for val in $zones; do
            if gcloud dataproc batches list --region=$val | grep -q "RUNNING"; then
                echo "There are running batches in $val"
            else
                echo "$val"
                break
            fi
            
        done
        """
        result = self.subprocess_hook.run_command(command=["bash", "-c", command])
        self.region = result.output
        self.logger.info(f"region --> {self.region}")

    def _create_batch(self):
        self.dataproc_hook.create_batch(
            region=self.region,
            project_id=self.project_id,
            batch=self.batch_config,
            batch_id=self.batch_id,
        )

    def _get_batch(self):
        def _internal_get_batch():
            batch = self.dataproc_hook.get_batch(
                region=self.region, project_id=self.project_id, batch_id=self.batch_id
            )
            return batch

        batch = _internal_get_batch()
        self.logger.info(f"Created Batch --> {batch}")

        while batch.state not in (
            Batch.State.FAILED,
            Batch.State.SUCCEEDED,
            Batch.State.CANCELLED,
        ):
            sleep(10)
            self.logger.info(
                f"Batch State --> {BATCH_STATE_ENUM_REFERENCE[batch.state]}"
            )
            batch = _internal_get_batch()

        self.final_batch_state = batch.state

    def _delete_batch(self):
        self.dataproc_hook.delete_batch(
            region=self.region, project_id=self.project_id, batch_id=self.batch_id
        )

    def _validate_result(self):
        if self.final_batch_state in (Batch.State.FAILED, Batch.State.CANCELLED):
            raise AirflowFailException(
                f"Final Batch State --> {BATCH_STATE_ENUM_REFERENCE[self.final_batch_state]}"
            )


class DataprocServerlessPlugin(AirflowPlugin):
    name = "Dataproc serverless Plugin"
    operators = [DataprocServerlessOperator]
