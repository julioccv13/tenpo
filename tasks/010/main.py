import argparse
from genderize import Genderize
import pandas as pd
import numpy as np

from core.ports import Process
from core.google import storage, bigquery

QUERY_PATH = "gs://{bucket_name}/010/queries/01_names.sql"
INSERT_PATH = "gs://{bucket_name}/010/queries/02_insert_names.sql"
DATA_QUERY = """
SELECT DISTINCT first_name as first_name 
FROM `{PROJECT_ID}.temp.P_{DS_NODASH}_Names`
"""
OUTPUT_TABLE = "temp.P_{DS_NODASH}_Out_Names"


class Process010(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._genderize_api_key = kwargs["genderize_api_key"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_target = kwargs["project_target"]

        self.generize_client = Genderize()

    def run(self):
        self._generate_names()
        self._get_raw_data()
        self._genderize_names()
        self._write_genderized_data()
        self._insert_query()

    def _generate_names(self):
        self._execute_query(path=QUERY_PATH)

    def _insert_query(self):
        self._execute_query(path=INSERT_PATH)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _get_raw_data(self):
        _query = DATA_QUERY.format(
            **{"PROJECT_ID": self._project_target, "DS_NODASH": self._ds_nodash}
        )
        self._raw_data = pd.read_gbq(
            _query, project_id=self._project_target, dialect="standard"
        )

    def _genderize_names(self):
        chunks = [[i + 1] * 10 for i in range(len(self._raw_data))]
        self._raw_data["group_id"] = np.resize(chunks, len(self._raw_data))
        self._raw_data = self._raw_data.groupby("group_id").agg(
            {"first_name": lambda x: list(x)}
        )
        gen = Genderize(api_key=self._genderize_api_key)

        self._raw_data["result"] = self._raw_data["first_name"].apply(
            lambda x: gen.get(x)
        )
        self._result_data = (
            self._raw_data.explode("result")["result"]
            .apply(pd.Series)
            .reset_index(drop=True)
        )

    def _write_genderized_data(self):
        output_table = OUTPUT_TABLE.format(**{"DS_NODASH": self._ds_nodash})
        self._result_data.to_gbq(
            output_table, project_id=self._project_target, if_exists="replace"
        )

    def _parse_query(self, query):
        return (
            query.replace(r"{{ds_nodash}}", self._ds_nodash)
            .replace(r"${project_source_1}", self._project_source_1)
            .replace(r"${project_source_2}", self._project_source_2)
            .replace(r"${project_source_3}", self._project_source_3)
            .replace(r"${project_target}", self._project_target)
            .format(**{"project_id": self._project_id})
        )


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-D", "--ds_nodash", type=str, required=True)
    parser.add_argument("-BN", "--bucket_name", type=str, required=True)
    parser.add_argument("-PI", "--project_id", type=str, required=True)
    parser.add_argument("-GA", "--genderize_api_key", type=str, required=True)
    parser.add_argument("-PS1", "--project_source_1", type=str, required=True)
    parser.add_argument("-PS2", "--project_source_2", type=str, required=True)
    parser.add_argument("-PS3", "--project_source_3", type=str, required=True)
    parser.add_argument("-PT", "--project_target", type=str, required=True)

    args = parser.parse_args()
    return vars(args)


if __name__ == "__main__":
    args = get_args()
    process = Process010(**args)
    process.run()
