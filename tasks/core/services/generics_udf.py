import unicodedata
import re
import json

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, MapType

from . import Udf


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')


def custom_regexp_replace(text, pattern, replacement):
    return re.sub(pattern, replacement, text)


def string_to_json(text):
    return json.loads(text)


def clear_string_to_json(text):
    data = json.loads(text)
    return {key: value for key, value in data.items() if value is not None}


def value_into_json_with_key(text, key):
    return {key: text}


strip_accents_udf = Udf("strip_accents",
                        udf(lambda text: strip_accents(text),
                            returnType=StringType()))

custom_regexp_udf = Udf("custom_regexp_replace",
                        udf(custom_regexp_replace,
                            returnType=StringType()))

string_to_json_udf = Udf("string_to_json",
                         udf(string_to_json,
                             returnType=MapType(StringType(),
                                                StringType())))

clear_string_to_json_udf = Udf("clear_string_to_json",
                               udf(clear_string_to_json,
                                   returnType=MapType(StringType(),
                                                      StringType())))

value_into_json_with_key_udf = Udf("value_into_json_with_key",
                                   udf(value_into_json_with_key,
                                       returnType=MapType(StringType(),
                                                          StringType())))

udf_list = [strip_accents_udf, custom_regexp_udf,
            string_to_json_udf, value_into_json_with_key_udf]
