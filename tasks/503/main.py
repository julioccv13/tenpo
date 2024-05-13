import argparse
import requests
import json

import pyspark.sql.functions as F
from pyspark.sql.types import MapType, StringType

from core.google import storage, bigquery
from core.spark import Spark


class Process503:

    def __init__(self, **kwargs):
        self._ds: str = kwargs["ds"]
        self._base_url = kwargs["clevertap_base_url"]
        self._bucket_name = kwargs["bucket_name"]
        self._clevertap_account_id = kwargs["clevertap_account_id"]
        self._clevertap_passcode = kwargs["clevertap_passcode"]
        self._project_id = kwargs["project_id"]
        self._project1 = kwargs["project1"]
        self._project3 = kwargs["project3"]
        self._project4 = kwargs["project4"]
        self._project9 = kwargs["project9"]
        self._env = kwargs["env"]
        self._common_headers = {
            "X-CleverTap-Account-Id": self._clevertap_account_id,
            "X-CleverTap-Passcode": self._clevertap_passcode,
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
        }

    def run(self):
        self._run_query()
        self._collect_data()
        self._validate_emails_clevertap()
        self._create_temp_view()
        self._insert_emails_clevertap()

    def _run_query(self):
        query_path = f"gs://{self._bucket_name}/503/queries/02_bq_users_clevertap.sql"
        query = storage.get_blob_as_string(query_path)
        query = query.replace(
            '${project3}', self._project3
        ).replace(
            '${project4}', self._project4
        ).replace(
            '${project9}', self._project9
        ).replace(
            '${ds_nodash}', self._ds.replace('-', '')
        ).replace(
            '${ds}', self._ds
        )
        bigquery.execute_query(query)

    def _collect_data(self):
        self.df = (
            Spark()
            .read
            .format("bigquery")
            .option("table",
                    f"tenpo-bi.temp.P_{self._ds.replace('-', '')}_Clevertap_Users")
            .load()
        )

    def _validate_emails_clevertap(self):
        method = "GET"
        constant_url = self._base_url
        constant_headers = self._common_headers

        def _get_response(email):
            path = f"1/profile.json?email={email}"
            response = requests.request(
                method=method,
                url=constant_url+path,
                headers=constant_headers,
            )
            return json.loads(response.text)

        f = F.udf(_get_response, returnType=MapType(StringType(), StringType()))

        self.df = self.df.withColumn(
            "response", f(F.col("email"))
        ).filter(
            F.col("response.record").isNotNull()
        ).select("email")

    def _create_temp_view(self):
        self.df.write \
            .format('bigquery') \
            .option('table',
                    f'{self._project_id}.temp.validated_clevertap_users_v2') \
            .option("temporaryGcsBucket", f"{self._env}-temporary-spark") \
            .mode('overwrite') \
            .save()

    def _insert_emails_clevertap(self):
        query_path = f"gs://{self._bucket_name}/503/queries/04_insert_emails.sql"
        query = storage.get_blob_as_string(query_path)
        query = query.replace(
            '${project1}', self._project1
        ).replace(
            '${project4}', self._project4
        ).replace(
            '${project9}', self._project9
        ).replace(
            '${ds_nodash}', self._ds.replace('-', '')
        )
        bigquery.execute_query(query)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-DS",
                        "--ds",
                        type=str,
                        required=True)
    parser.add_argument("-CBU",
                        "--clevertap-base-url",
                        type=str,
                        required=True)
    parser.add_argument("-BN",
                        "--bucket-name",
                        type=str,
                        required=True)
    parser.add_argument("-CAI",
                        "--clevertap-account-id",
                        type=str,
                        required=True)
    parser.add_argument("-CAP",
                        "--clevertap-account-passcode",
                        type=str,
                        required=True)
    parser.add_argument("-P1",
                        "--project1",
                        type=str,
                        required=True)
    parser.add_argument("-PI",
                        "--project-id",
                        type=str,
                        required=True)
    parser.add_argument("-P3",
                        "--project3",
                        type=str,
                        required=True)
    parser.add_argument("-P4",
                        "--project4",
                        type=str,
                        required=True)
    parser.add_argument("-P9",
                        "--project9",
                        type=str,
                        required=True)
    parser.add_argument("-ENV",
                        "--env",
                        type=str,
                        required=True)
    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    args = get_args()
    process = Process503(**args)

    process.run()
