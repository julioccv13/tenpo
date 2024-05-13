import unicodedata
import re
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime

from pyspark.sql.types import (
    DataType,
    IntegerType,
    FloatType,
    StringType,
    BooleanType,
    TimestampType
)


def fix_key(cname: str) -> str:
    BADCHAR_RE = r'( |x|\(|\))'
    REPEAT_UNDERSCORE_RE = r'_+'
    UNDERSCORE_END_RE = r'_$'
    UNDERSCORE_START_RE = r'^_'

    def strip_accents(s): return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn')
    cname = cname.lower()
    cname = strip_accents(cname)
    cname = re.sub(BADCHAR_RE, '_', cname)
    cname = re.sub(REPEAT_UNDERSCORE_RE, '_', cname)
    cname = re.sub(UNDERSCORE_END_RE, '', cname)
    cname = re.sub(UNDERSCORE_START_RE, '', cname)

    return cname


def get_deep_value_or_null(dict_value: Dict[Any, Any],
                           keys: List[Any],
                           default: Optional[Any] = None,
                           caster: Optional[Callable] = None) -> Any:
    try:
        for k in keys:
            dict_value = dict_value[k]
        if caster:
            return caster(dict_value)
        return dict_value
    except KeyError:
        return default


def spark_translate_type(type_: type) -> DataType:
    if type_ == int:
        return IntegerType()
    elif type_ == float:
        return FloatType()
    elif type_ == bool:
        return BooleanType()
    elif type_ == datetime:
        return TimestampType()
    else:
        return StringType()


def parse_string_date_with_timezone(date_string: str) -> datetime:
    return datetime.strptime(date_string.replace(
            "T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")