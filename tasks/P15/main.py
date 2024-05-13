import argparse
import requests
import json
from core.google import bigquery

from core.ports import Process
from core.spark import Spark
from core.models.typeform import Item, HiddenItem, AnswersItem
from core.common import parse_string_date_with_timezone


COOKIE_HEADER = (
    "AWSALBTG=opsOBOvOO/"
    "dX4s1x6JQYz1eBvR7vko6rrZ85RnGHZevpKOUjnZcIoL4WR22fnQhj9BZQFjwsVmWf/"
    "1RQotg6cSRQFMk0ZvRCxweX2UcDIRYGZw83Cx3R+ENQ7iwcPLe8Zovp+/NtwRunrC/"
    "GA4GbAr2XQNcEsUQR5XLmMoqFH1rI; "
    "AWSALBTGCORS=opsOBOvOO/"
    "dX4s1x6JQYz1eBvR7vko6rrZ85RnGHZevpKOUjnZcIoL4WR22fnQhj9BZQFjwsVmWf/"
    "1RQotg6cSRQFMk0ZvRCxweX2UcDIRYGZw83Cx3R+ENQ7iwcPLe8Zovp+/"
    "NtwRunrC/GA4GbAr2XQNcEsUQR5XLmMoqFH1rI"
)

TYPEFORM_URL = (
    "{url}/forms/{form_id}/responses"
    "?since={since}&until={until}"
    "&page_size={page_size}&completed=true"
)
TYPEFORM_BEFORE_URL = (
    "{url}/forms/{form_id}/responses"
    "?since={since}&until={until}"
    "&page_size={page_size}&before={before}"
    "&completed=true"
)
PAGE_SIZE_GET_FORMS = 200
PAGE_SIZE_TYPEFORM = 1000
TYPEFORM_GET_FORMS_URL = "{url}/forms?page={page}&page_size={page_size}"
ITEMS_PATH = "gs://{bucket_name}/p15/items.parquet"
ANSWERS_PATH = "gs://{bucket_name}/p15/answers.parquet"
DELETE_ITEMS_QUERY = """
DELETE FROM `{project_id}.typeform.typeform_item` 
WHERE (submitted_at >= '{since}' 
AND submitted_at <= '{until}') AND form_id = '{form_id}';
"""
DELETE_ITEM_ANSWERS = """
DELETE FROM `{project_id}.typeform.typeform_item_answer` 
WHERE (submitted_at >= '{since}' 
AND submitted_at <= '{until}') AND form_id = '{form_id}';
"""
ITEMS_TABLE_NAME = "{project_id}.typeform.typeform_item"
ANSWERS_TABLE_NAME = "{project_id}.typeform.typeform_item_answer"


class Typeform:
    def __init__(self, **kwargs):
        self._typeform_url = kwargs["url"]

        self._typeform_account_passcode = kwargs["typeform_account_passcode"]

        self._since = kwargs["since"]
        self._until = kwargs["until"]

        self._common_headers = {
            "Content-Type": "text/plain",
            "Accept": "application/json",
            "Authorization": "Bearer " + self._typeform_account_passcode,
            "Cookie": COOKIE_HEADER,
        }

    def get_forms(self):
        forms = []
        page = 1
        while True:
            url = TYPEFORM_GET_FORMS_URL.format(url=self._typeform_url,
                                                page=page,
                                                page_size=PAGE_SIZE_GET_FORMS)
            response = self._get_response(url, 'GET')
            temp_forms_id = [items['id'] for items in response['items']]
            page += 1
            if response['page_count'] < page:
                break
            forms.extend(temp_forms_id)
        return forms

    def get_data(self, forms):
        _rdd = Spark().sparkContext.parallelize(forms)
        _data = (
            _rdd
            .map(lambda form: self._get_data(form))
            .filter(lambda x: x is not None)
        )
        _answers = _data.flatMap(lambda x: x['data_answers']).flatMap(lambda x: x)
        _items = _data.flatMap(lambda x: x['data_items'])

        return _items, _answers

    def _get_data(self, form):
        _last_item_id = ''
        _data_items = []
        _data_answers = []
        _response = self._get_response(self._build_url(form), 'GET')
        self._total_items = _response['total_items']
        self._total_pages = _response['page_count']
        self._count_pages = 1
        self._total_count_items = 1
        while self._count_pages < self._total_pages + 1:
            self._count_items = 1
            if self._count_pages > 1:
                _response = self._get_response(
                    self._build_url(form, _last_item_id), 'GET')

            _typeform_items, _typeform_answers = self._process_response(_response, form)
            _data_items.extend(_typeform_items)
            _data_answers.extend(_typeform_answers)
        if _data_items or _data_answers:
            return {"data_items": _data_items, "data_answers": _data_answers}

    def _process_response(self, response, form):
        _typeform_items = []
        _typeform_answers = []
        for _item in response['items']:
            _typeform_items.append(self._parse_item(_item, form))
            _answer = self._safe_parse_answer(_item, form)
            if _answer:
                _typeform_answers.append(_answer)

            if self._count_items == PAGE_SIZE_TYPEFORM:
                self._last_item_id = _item['landing_id']
                self._count_pages += 1
                break
            elif self._total_count_items == self._total_items:
                self._count_pages += 1
                break
            else:
                self._count_items += 1
                self._total_count_items += 1
        return _typeform_items, _typeform_answers

    def _parse_item(self, item, form):
        return Item.parse(item, form)

    def _safe_parse_answer(self, item, form):
        if item['answers']:
            return AnswersItem.parse(item, form)
        elif item['hidden']:
            return HiddenItem.parse(item, form)

    def _get_response(self, url, method):
        response = requests.request(
            method=method,
            url=url,
            headers=self._common_headers,
        )
        return json.loads(response.text)

    def _build_url(self, form, before=None):
        kwargs = {
            "url": self._typeform_url,
            "form_id": form,
            "since": self._since,
            "until": self._until,
            "page_size": PAGE_SIZE_TYPEFORM,
        }
        if before:
            kwargs["before"] = before
            url = TYPEFORM_BEFORE_URL
        else:
            url = TYPEFORM_URL
        return url.format(**kwargs)


class ProcessP15(Process):
    def __init__(self, **kwargs):
        self._typeform = Typeform(**kwargs)
        self._since = parse_string_date_with_timezone(kwargs["since"])
        self._until = parse_string_date_with_timezone(kwargs["until"])
        self._project_id = kwargs["project_id"]
        self._bucket_name = kwargs["bucket_name"]

    def run(self):
        self._forms = self._typeform.get_forms()
        self._extract_data(self._forms)
        self._clear_previous_data()
        self._load()

    def _extract_data(self, forms):
        self._items, self._answers = self._typeform.get_data(forms)

    def _load(self):
        self._load_items()
        self._load_answers()

    def _load_items(self):
        self._load_parquet_to_bq(
            self._items,
            self._build_path(ITEMS_PATH),
            Item.schema(),
            self._build_table_name(ITEMS_TABLE_NAME),
        )

    def _load_answers(self):
        self._load_parquet_to_bq(
            self._answers,
            self._build_path(ANSWERS_PATH),
            AnswersItem.schema(),
            self._build_table_name(ANSWERS_TABLE_NAME),
        )

    def _build_table_name(self, table_name):
        return table_name.format(project_id=self._project_id)

    def _build_path(self, path):
        return path.format(bucket_name=self._bucket_name)

    def _clear_previous_data(self):
        for _form in self._forms:
            self._delete_form_data(_form)

    def _delete_form_data(self, form):
        self._delete_item_answers(form)
        self._delete_item_data(form)

    def _delete_item_answers(self, form):
        query = DELETE_ITEM_ANSWERS.format(
            project_id=self._project_id,
            since=self._since,
            until=self._until,
            form_id=form,
        )
        bigquery.execute_query(query)

    def _delete_item_data(self, form):
        query = DELETE_ITEMS_QUERY.format(
            project_id=self._project_id,
            since=self._since,
            until=self._until,
            form_id=form,
        )
        bigquery.execute_query(query)

    def _load_parquet_to_bq(self, data, path, schema, table_name):
        if not data.isEmpty():
            self._load_parquet(data, path, schema)
            bigquery.upload_to_bigquery_stage_file(
                table_name,
                path + "/part*",
                write_disposition="WRITE_APPEND")

    def _load_parquet(self, data, path, schema):
        (
            data
            .toDF(schema)
            .write
            .format("parquet")
            .mode("overwrite")
            .save(path)
        )


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-U",
                        "--url",
                        type=str,
                        required=True)
    parser.add_argument("-TAP",
                        "--typeform-account-passcode",
                        type=str,
                        required=True)
    parser.add_argument("-S",
                        "--since",
                        type=str,
                        required=True)
    parser.add_argument("-u",
                        "--until",
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
    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    args = get_args()
    process = ProcessP15(**args)

    process.run()
