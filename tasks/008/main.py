import argparse
import requests

from pyspark.sql.functions import lit, col, ceil, collect_list
from pyspark.sql.types import IntegerType

from core.ports import Process
from core.spark import Spark
from core.google import storage, bigquery
from core.services.factory_udf import process_json_key_custom_function_udf_factory
from core.services.generics_udf import string_to_json_udf, value_into_json_with_key_udf

MAX_RECORDS_PER_PAYLOAD = 100
QUERIES_PREFIX = "gs://{bucket_name}/008a/queries/"
PARAMETERS_QUERY_PATH = QUERIES_PREFIX + "01_parametros.sql"
INSERT_QUERY_PATH = QUERIES_PREFIX + "03_insert.sql"
PERIOD_QUERIES = {
    "daily": QUERIES_PREFIX + "periods/02_daily.sql",
    "hourly": QUERIES_PREFIX + "periods/02_hourly.sql",
    "weekly": QUERIES_PREFIX + "periods/02_weekly.sql",
}
SOURCE_DATA_TABLE = "${project_id}:temp.clevertap_injection_{{period}}_{{ds_nodash}}"

KEY_TRANSFORM = {
    'date_of_birth': 'Birthday',
    'saldo': 'Saldo',
    'saldo_bolsillo': 'Saldo Bolsillo',
    'no_molestar': 'No molestar',
    'tenpo_user_persona': 'User Persona',
    'whitelist_iyg': 'Beta invita y gana'
}


class Process008(Process):
    def __init__(self, **kwargs):
        self._period = kwargs["period"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._clevertap_url = kwargs["clevertap_url"]
        self._bucket_name = kwargs["bucket_name"]
        self._project_id = kwargs["project_id"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_source_4 = kwargs["project_source_4"]
        self._project_target = kwargs["project_target"]
        self._clevertap_account_id = kwargs["clevertap_account_id"]
        self._clevertap_passcode = kwargs["clevertap_account_passcode"]
        self._common_headers = {
            "X-CleverTap-Account-Id": self._clevertap_account_id,
            "X-CleverTap-Passcode": self._clevertap_passcode,
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        }

    def run(self):
        self._extract_data()
        self._transform_data()
        self._load_data()

    def _extract_data(self):
        self._generate_parameters()
        self._generate_properties()
        self._data = self._read_data_from_bigquery()

    def _transform_data(self):
        self._build_clevertap_profile_payload_from_data()

    def _load_data(self):
        self._send_payload_to_clevertap()
        self._insert_data_to_bigquery()

    def _generate_parameters(self):
        self._execute_query(path=PARAMETERS_QUERY_PATH)

    def _generate_properties(self):
        self._execute_query(path=PERIOD_QUERIES[self._period])

    def _read_data_from_bigquery(self):
        _query = self._parse_query(SOURCE_DATA_TABLE)
        return (
            Spark()
            .read
            .format("bigquery")
            .option("table", _query)
            .load()
        ).selectExpr("identity", "'profile' as type", "profileData")

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return (query
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .replace(r"{{period}}", self._period)
                .replace(r"${project_source1}", self._project_source_1)
                .replace(r"${project_source2}", self._project_source_2)
                .replace(r"${project_source3}", self._project_source_3)
                .replace(r"${project_source4}", self._project_source_4)
                .replace(r"${project_target}", self._project_target)
                .replace(r"${project_id}", self._project_id)
                )

    def _build_clevertap_profile_payload_from_data(self):
        self._process_data()
        self._build_chunks()

    def _process_data(self):
        self._data = (
            self._data
            .withColumn("payload", string_to_json_udf.function("profileData"))
            .withColumn("payload",
                        process_json_key_custom_function_udf_factory(
                            lambda value: KEY_TRANSFORM.get(value, value)
                        ).function("payload"))
        )

    def _build_chunks(self):
        _rdd = self._data.select("payload").rdd
        _rdd = _rdd.zipWithIndex()
        self._data = _rdd.toDF(["payload", "id"]).select("id", "payload.*")
        self._data.cache()
        self._data = self._data.withColumn(
            "id",
            ceil(
                (col("id") + lit(1)) /
                lit(MAX_RECORDS_PER_PAYLOAD)
            ).cast(IntegerType()))
        self._data = (
            self._data
            .groupBy("id")
            .agg(
                collect_list("payload").alias("payload")
            ).withColumn(
                "payload",
                value_into_json_with_key_udf.function("payload", lit("d"))
            )
        )

    def _send_payload_to_clevertap(self):
        _collected_data = self._data.collect()
        for row in _collected_data:
            self._send_payload(row)

    def _send_payload(self, row):
        requests.request(
            method='POST',
            url=self._clevertap_url,
            headers=self._common_headers,
            data=row.payload)

    def _insert_data_to_bigquery(self):
        self._execute_query(path=INSERT_QUERY_PATH)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-P",
                        "--period",
                        type=str,
                        enum=["daily", "hourly", "weekly"],
                        required=True)
    parser.add_argument("-D",
                        "--ds-nodash",
                        type=str,
                        required=True)
    parser.add_argument("-CU",
                        "--clevertap-url",
                        type=str,
                        required=True)
    parser.add_argument("-BN",
                        "--bucket-name",
                        type=str,
                        required=True)
    parser.add_argument("-PI",
                        "--project-id",
                        type=str,
                        required=True)
    parser.add_argument("-PT",
                        "--project-target",
                        type=str,
                        required=True)
    parser.add_argument("-PS1",
                        "--project-source-1",
                        type=str,
                        required=True)
    parser.add_argument("-PS2",
                        "--project-source-2",
                        type=str,
                        required=True)
    parser.add_argument("-PS3",
                        "--project-source-3",
                        type=str,
                        required=True)
    parser.add_argument("-PS4",
                        "--project-source-4",
                        type=str,
                        required=True)

    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    args = get_args()
    process = Process008(**args)

    process.run()
