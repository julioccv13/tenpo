import argparse
import json
import logging
import requests

from core.ports import Process
from core.spark import Spark
from core.google import storage, bigquery
from core.models.clevertap import (
    DataUniqueRequestModel,
    DataTrendResponseModel,
    DataRequestModel,
    EventModel,
    AppInstalledModel,
    DataOnfidoModel
)

EVENTS_PATH = "gs://{bucket_name}/P16/events.json"
TRENDS_PATH = "gs://{bucket_name}/P16/trends.parquet"
APP_INSTALLED_PATH = "gs://{bucket_name}/P16/app_ins.parquet"
ONFIDO_PATH = "gs://{bucket_name}/P16/onfido.parquet"
EVENTS_DATA_PATH = "gs://{bucket_name}/P16/event_data.parquet"

TRENDS_TABLE = "{project_id}.temp.trends_{end_date}"
APP_INSTALLED_TABLE = "{project_id}.temp.app_ins_{end_date}"
ONFIDO_TABLE = "{project_id}.temp.onfido_{end_date}"
EVENTS_DATA_TABLE = "{project_id}.temp.events_data_{end_date}"

# TODO: Hardcoded dataset
PROCESS_TRENDS_QUERY = (
    "DELETE FROM `{project_id}.{dataset}.trends` WHERE date"
    " >= {start_date} AND date <= {end_date};"
    " INSERT INTO `{project_id}.{dataset}.trends`"
    " SELECT DISTINCT * FROM `{temp_trends_table}`;"
)
# TODO: Hardcoded dataset
PROCESS_EVENTS_DATA_QUERY = (
    "DELETE FROM `{project_id}.{dataset}.events` WHERE date"
    " >= {start_date} AND date <= {end_date};"
    " INSERT INTO `{project_id}.{dataset}.events`"
    " SELECT DISTINCT * FROM `{temp_events_table}`;"
    " DELETE FROM `{project_id}.{dataset}.events_app_install`"
    " WHERE date >= {start_date} AND date <= {end_date};"
    " INSERT INTO `{project_id}.{dataset}.events_app_install`"
    " SELECT DISTINCT * FROM `{temp_app_install_events_table}`;"
    " DELETE FROM `{project_id}.{dataset}.events_session_concluded`"
    " WHERE date >= {start_date} AND date <= {end_date};"
    " INSERT INTO `{project_id}.{dataset}.events_session_concluded`"
    " SELECT DISTINCT * FROM `{temp_session_concluded_events}`;"
)


class EventsFetcher:
    def __init__(self,
                 events,
                 from_,
                 to,
                 url,
                 clevertap_account_id,
                 clevertap_account_passcode):
        self._events = events
        self._clevertap_url = url

        self._clevertap_account_id = clevertap_account_id
        self._clevertap_account_passcode = clevertap_account_passcode
        self._common_headers = {
            "X-CleverTap-Account-Id": self._clevertap_account_id,
            "X-CleverTap-Passcode": self._clevertap_account_passcode,
            "Content-Type": "application/json",
        }
        self._from_ = from_
        self._to = to


class Trends(EventsFetcher):
    def __init__(self,
                 events,
                 from_,
                 to,
                 trends_url,
                 clevertap_account_id,
                 clevertap_account_passcode):
        super().__init__(events,
                         from_,
                         to,
                         trends_url,
                         clevertap_account_id,
                         clevertap_account_passcode)
        self._clevertap_trends_items_url = self._clevertap_url + "?req_id={req_id}"

    def get_data(self):
        rdd = Spark().sparkContext.parallelize(self._events)
        return rdd.flatMap(lambda event: self._safe_process_trend_event(event))

    def _safe_process_trend_event(self, event):
        try:
            return self._process_trend_event(event)
        except Exception as e:
            logging.error("Event: {} failed with error: {}"
                          .format(event['name'], str(e)))
            return []

    def _process_trend_event(self, event):
        response_data = self._send_trend_payload(event)
        response_items = self._get_trend_data(event, response_data)
        return response_items

    def _send_trend_payload(self, event):
        event_data = DataUniqueRequestModel.parse(event, self._from_, self._to)
        response = requests.request(
            method='POST',
            url=self._clevertap_url,
            headers=self._common_headers,
            data=event_data)
        return json.loads(response.text)

    def _get_trend_data(self, event, response_data):
        if response_data['status'] != 'partial':
            return []
        req_id = response_data['req_id']
        response = requests.request(
            method='GET',
            url=self._clevertap_trends_items_url.format(req_id=req_id),
            headers=self._common_headers)
        trend_data_list = json.loads(response.text)
        return DataTrendResponseModel.parse(event, trend_data_list)


class EventsData(EventsFetcher):
    def __init__(self,
                 events,
                 from_,
                 to,
                 events_url,
                 clevertap_account_id,
                 clevertap_account_passcode):
        super().__init__(events,
                         from_,
                         to,
                         events_url,
                         clevertap_account_id,
                         clevertap_account_passcode)
        self._clevertap_cursor_url = self._clevertap_url + "?cursor={cursor}"

    def get_data(self,):
        rdd = Spark().sparkContext.parallelize(self._events)
        rdd = rdd.map(lambda event: self._safe_process_event(event))
        app_ins = rdd.filter(lambda x: x['event'] == 'App Installed'
                             ).flatMap(lambda x: x['records'])
        onfido = rdd.filter(lambda x: x['event']
                            == 'Validación de identidad por Onfido completada'  # TODO: No hay evento de Onfido
                            ).flatMap(lambda x: x['records'])
        data = rdd.filter(lambda x: x['event']
                          not in ['App Installed',
                                  'Validación de identidad por Onfido completada']
                          ).flatMap(lambda x: x['records'])
        return app_ins, onfido, data

    def _safe_process_event(self, event):
        try:
            return self._process_event(event)
        except Exception as e:
            logging.error("Event: {} failed with error: {}"
                          .format(event['name'], str(e)))
            return {"event": event['name'], "records": []}

    def _process_event(self, event):
        response_data = self._send_event_payload(event)
        response_items = self._get_event_data(event, response_data)
        return response_items

    def _send_event_payload(self, event):
        event_data = DataRequestModel.parse(event, self._from_, self._to)
        response = requests.request(
            method='POST',
            url=self._clevertap_url,
            headers=self._common_headers,
            data=event_data)
        if response.status_code != 200:
            return {}
        return json.loads(response.text)

    def _get_event_data(self, event, response_data):
        data_items = []
        if response_data.get('status') == 'success':
            exists_cursor, cursor = self._validate_cursor(response_data)
            while(exists_cursor):
                response_data = self._send_cursor_payload(cursor)
                if response_data:
                    new_data_items = self._parse_cursor_query_result(
                        event, response_data)
                    data_items.extend(new_data_items)
                exists_cursor, cursor = self._validate_cursor(
                    response_data, "next_cursor")
        return {"event": event['name'], "records": data_items}

    def _send_cursor_payload(self, cursor):
        response = requests.request(
            method="GET",
            url=self._clevertap_cursor_url.format(cursor=cursor),
            headers=self._common_headers)
        if response.status_code != 200:
            return None
        response_data = json.loads(response.text)
        if response_data['status'] != 'success':
            return None
        return response_data

    def _parse_cursor_query_result(self, event, response_data):
        data_items = []
        for data in response_data.get('records', []):
            event_name = event['name']
            data['event'] = event_name
            if event_name == 'App Installed':
                data_item = AppInstalledModel.parse(data)
            elif event_name == 'Validación de identidad por Onfido completada':
                data_item = DataOnfidoModel.parse(data)
            else:
                data_item = EventModel.parse(data)
            data_items.append(data_item)
        return data_items

    def _validate_cursor(self, data, cursor_key="cursor"):
        data = data if data else {}
        cursor = data.get(cursor_key)
        if not cursor:
            return False, None
        return True, cursor


class ProcessP16(Process):
    def __init__(self, **kwargs):
        self._start_date = kwargs.get('start_date')
        self._end_date = kwargs.get('end_date')
        self._trends_url = kwargs["trends_url"]
        self._events_data_url = kwargs["events_data_url"]

        self._clevertap_account_id = kwargs["clevertap_account_id"]
        self._clevertap_account_passcode = kwargs["clevertap_account_passcode"]

        self._bigquery_dataset = kwargs["bigquery_dataset_name"]
        self._bucket_name = kwargs["bucket_name"]
        self._project_id = kwargs["project_id"]

        self._trends_table_name = self._build_table_name(TRENDS_TABLE)
        self._events_data_table_name = self._build_table_name(EVENTS_DATA_TABLE)
        self._app_installed_table_name = self._build_table_name(APP_INSTALLED_TABLE)
        self._onfido_table_name = self._build_table_name(ONFIDO_TABLE)

        self._events_path = self._build_gcs_paths(EVENTS_PATH)
        self._trends_files_path = self._build_gcs_paths(TRENDS_PATH)
        self._events_data_files_path = self._build_gcs_paths(EVENTS_DATA_PATH)
        self._app_installed_files_path = self._build_gcs_paths(APP_INSTALLED_PATH)
        self._onfido_files_path = self._build_gcs_paths(ONFIDO_PATH)

        self._events = self._get_events()

        self._trends = Trends(
            self._events,
            int(self._start_date),
            int(self._end_date),
            self._trends_url,
            self._clevertap_account_id,
            self._clevertap_account_passcode
        )
        self._events_data = EventsData(
            self._events,
            int(self._start_date),
            int(self._end_date),
            self._events_data_url,
            self._clevertap_account_id,
            self._clevertap_account_passcode
        )

    def run(self):
        self._extract_data()
        self._load_data()
        self._transform_data()

    def _extract_data(self):
        self._trends_data = self._trends.get_data()
        self._app_ins, self._onfido, self._event_data = self._events_data.get_data()

    def _transform_data(self):
        trends_query = PROCESS_TRENDS_QUERY.format(
            **{"project_id": self._project_id,
               "dataset": self._bigquery_dataset,
                "start_date": self._start_date,
                "end_date": self._end_date,
                "temp_trends_table": self._trends_table_name,
               }
        )
        events_data_query = PROCESS_EVENTS_DATA_QUERY.format(
            **{"project_id": self._project_id,
               "dataset": self._bigquery_dataset,
                "start_date": self._start_date,
                "end_date": self._end_date,
                "temp_events_table": self._events_data_table_name,
                "temp_app_install_events_table": self._app_installed_table_name,
                "temp_session_concluded_events": self._onfido_table_name,
               }
        )
        bigquery.execute_query(trends_query)
        bigquery.execute_query(events_data_query)

    def _build_table_name(self, table_name_template):
        return table_name_template.format(
            **{"project_id": self._project_id,
               "end_date": self._end_date,
               })

    def _build_gcs_paths(self, path_template):
        return path_template.format(
            **{"bucket_name": self._bucket_name}
        )

    def _load_data(self):
        self._load_parquet_to_bq(
            self._trends_data,
            self._trends_files_path,
            DataTrendResponseModel.schema(),
            self._trends_table_name)
        self._load_parquet_to_bq(
            self._app_ins,
            self._app_installed_files_path,
            AppInstalledModel.schema(),
            self._app_installed_table_name)
        self._load_parquet_to_bq(
            self._onfido,
            self._onfido_files_path,
            DataOnfidoModel.schema(),
            self._onfido_table_name)
        self._load_parquet_to_bq(
            self._event_data,
            self._events_data_files_path,
            EventModel.schema(),
            self._events_data_table_name)

    def _load_parquet_to_bq(self, data, path, schema, table_name):
        if not data.isEmpty():
            self._load_parquet(data, path, schema)
            bigquery.upload_to_bigquery_stage_file(table_name, path + "/part*")

    def _load_parquet(self, data, path, schema):
        (
            data
            .toDF(schema)
            .write
            .format("parquet")
            .mode("overwrite")
            .save(path)
        )

    def _get_events(self):
        events_string = storage.get_blob_as_string(self._events_path)
        return json.loads(events_string)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-SD",
                        "--start-date",
                        type=str,
                        required=True)
    parser.add_argument("-ED",
                        "--end-date",
                        type=str,
                        required=True)
    parser.add_argument("-TU",
                        "--trends-url",
                        type=str,
                        required=True)
    parser.add_argument("-EU",
                        "--events-data-url",
                        type=str,
                        required=True)
    parser.add_argument("-CID",
                        "--clevertap-account-id",
                        type=str,
                        required=True)
    parser.add_argument("-CP",
                        "--clevertap-account-passcode",
                        type=str,
                        required=True)
    parser.add_argument("-BQD",
                        "--bigquery-dataset-name",
                        type=str,
                        required=True)
    parser.add_argument("-BCK",
                        "--bucket-name",
                        type=str,
                        required=True)
    parser.add_argument("-PRJ",
                        "--project-id",
                        type=str,
                        required=True)

    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    args = get_args()
    process = ProcessP16(**args)

    process.run()
