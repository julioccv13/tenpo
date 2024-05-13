from datetime import datetime
from dataclasses import dataclass

from core.models.parseable import Parseable
from core.common import parse_string_date_with_timezone


@dataclass
class Item(Parseable):
    form_id: str
    item_id: str
    landed_at: datetime
    submitted_at: datetime

    @classmethod
    def parse(cls, item, form):
        landing_id = item["landing_id"]
        landed_at = parse_string_date_with_timezone(item["landed_at"])
        submitted_at = parse_string_date_with_timezone(item["submitted_at"])
        return {
            "form_id": str(form),
            "item_id": str(landing_id),
            "landed_at": landed_at,
            "submitted_at": submitted_at
        }


@dataclass
class AnswersItem(Parseable):
    form_id: str
    item_id: str
    field_id: str
    field_ref: str
    field_type: str
    answer_type: str
    answer: str
    submitted_at: str

    def __init__(self, item, form):
        self._item = item
        self._form = form

    @classmethod
    def parse(cls, item, form):
        answers = []
        _instance = cls(item, form)
        for _answer in item["answers"]:
            answers.append(_instance._parse_answer(_answer))
        return answers

    def _parse_answer(self, answer):
        field_id = answer["field"]["id"]
        field_ref = answer["field"]["ref"]
        answer_type = answer["type"]
        field_type = answer["field"]["type"]
        new_answer = ""
        if field_type == "opinion_scale" or field_type == "rating":
            new_answer = answer["number"]
        elif field_type == "multiple_choice":
            new_answer = self._parse_multiple_choice_answer(answer)
        elif field_type == "short_text" and answer_type == "text":
            new_answer = answer["text"]
        elif field_type == "email" and answer_type == "email":
            new_answer = answer["email"]
        submitted_at = parse_string_date_with_timezone(
            self._item["submitted_at"])
        return {
            "form_id": str(self._form),
            "item_id": str(self._item["landing_id"]),
            "field_id": str(field_id),
            "field_ref": str(field_ref),
            "field_type": str(field_type),
            "answer_type": str(answer_type),
            "answer": str(new_answer),
            "submitted_at": str(submitted_at),
        }

    def _parse_multiple_choice_answer(self, answer):
        new_answer = ""
        answer_type = answer["type"]
        choices = ["label", "labels", "other"]
        if answer_type in ("choice", "choices"):
            for choice in choices:
                if choice in answer[answer_type]:
                    new_answer = ",".join(
                        map(str, answer[answer_type][choice]))
                    break
        else:
            answer_type_id = ",".join(
                map(str, answer["choices"]["ids"]))
            if answer_type_id in ("LEtG2ygnbRNw", "tEquVETwbV90"):
                new_answer = ",".join(
                    map(str, answer["choices"]["labels"]))
            elif answer_type_id == "other":
                new_answer = answer["choices"]["other"]

        return new_answer


class HiddenItem(AnswersItem):

    @classmethod
    def parse(cls, item, form):
        submitted_at = parse_string_date_with_timezone(item["submitted_at"])
        return {
            "form_id": str(form),
            "item_id": str(item["landing_id"]),
            "field_id": "hidden",
            "field_ref": "hidden",
            "field_type": "hidden",
            "answer_type": "hidden",
            "answer": str(item["hidden"]["id"]),
            "submitted_at": str(submitted_at),
        }
