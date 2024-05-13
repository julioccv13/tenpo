import json
import requests
import pandas
from core.ports import Process
from core.spark import Spark
from core.google import storage, bigquery

SEGMENT_TABLE_TEMPLATE = "{project_source_5}.temp.{{segment_name}}_{{ds_nodash}}"

CREATE_QUERY_PATH = "gs://{bucket_name}/110/queries"

CREATE_QUERY_TEMPLATE = """
DROP TABLE IF EXISTS `{project_source_5}.temp.{segment_name}_{{ds_nodash}}`;
CREATE TABLE `{project_source_5}.temp.{segment_name}_{{ds_nodash}}` AS ({query});"""


class ExistenceError(Exception):
    pass

class Process110(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_source_4 = kwargs["project_source_4"]
        self._project_source_5 = kwargs["project_source_5"]
        self._project_id = kwargs["project_id"]
        self._clevertap_presigned_url = kwargs["clevertap_presigned_url"]
        self._clevertap_url = kwargs["clevertap_url"]
        self._clevertap_account_id = kwargs["clevertap_account_id"]
        self._clevertap_passcode = kwargs["clevertap_passcode"]
        self._clevertap_auth = kwargs["clevertap_auth"]
        self._common_headers = {
            "X-CleverTap-Account-Id": self._clevertap_account_id,
            "X-CleverTap-Passcode": self._clevertap_passcode,
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Authorization": "Basic " + self._clevertap_auth,
        }

    def run(self):
        segment_names = self._create_temp_tables(path=CREATE_QUERY_PATH)
        for segment in segment_names:
            data = self._read_data_from_bigquery(segment)
            presigned_url = self._get_presigned_url()
            self._push_data(data, presigned_url)
            response = self._safe_post_event(segment, presigned_url)
            print("Segment ", segment)
            print("final response:", response)
            print("Response text", response.text)

    def _create_temp_tables(self, path):
        segments = []
        queries_path = path.format(**{"bucket_name": self._bucket_name})
        list_of_queries = storage.get_blobs_list_prefix_path_as_strings(queries_path)
        for query_pair in list_of_queries:
            segment, query = self._format_create_query(query_pair)
            if not segment:
                continue
            segments.append(segment)
            self._execute_query(query=query)
        return segments

    def _format_create_query(self, query_pair):
        name = query_pair[0]
        query = query_pair[1]
        segment_name = name.split("/")[-1].split(".")[0]
        return segment_name, CREATE_QUERY_TEMPLATE.format(
            project_source_5=self._project_source_5,
            segment_name=segment_name,
            query=query,
        )

    def _read_data_from_bigquery(self, segment):
        _query = self._get_segment_query(segment)
        _query = self._parse_query(_query)
        return pandas.read_gbq(_query, project_id=self._project_id)

    def _get_segment_query(self, segment):
        return SEGMENT_TABLE_TEMPLATE.replace(r"{{segment_name}}", segment)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return query.replace(r"{{ds_nodash}}", self._ds_nodash).format(
            **{
                "project_source_1": self._project_source_1,
                "project_source_2": self._project_source_2,
                "project_source_3": self._project_source_3,
                "project_source_4": self._project_source_4,
                "project_source_5": self._project_source_5,
                "ds_nodash": self._ds_nodash,
            }
        )

    def _get_presigned_url(self):
        return requests.post(
            self._clevertap_presigned_url, headers=self._common_headers
        ).json()["presignedS3URL"]

    def _push_data(self, data, presigned_url):
        requests.put(presigned_url, data.to_csv(None, index=False))

    def _post_event(self, segment, presigned_url, replace=False):
        event = {
            "name": segment,
            "email": "crm@tenpo.cl",
            "filename": f"{segment}.csv",
            "creator": "110_segmentos_daily",
            "url": presigned_url,
            "replace": replace,
        }
        response = requests.post(
            self._clevertap_url, headers=self._common_headers, data=json.dumps(event)
        )
        if (
            response.status_code == 401
            and "Duplicate custom segment name" in response.text
        ):
            raise ExistenceError(f"{segment} already exists")
        return response

    def _safe_post_event(self, segment, presigned_url):
        try:
            return self._post_event(segment, presigned_url)
        except ExistenceError:
            return self._post_event(segment, presigned_url, True)
