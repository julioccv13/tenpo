from typing import Dict, Any, get_args
from typing_inspect import is_optional_type, is_union_type

from pyspark.sql.types import StructType, StructField

from core.common import spark_translate_type


class Parseable:
    @classmethod
    def parse(cls):
        raise NotImplementedError

    @classmethod
    def schema(cls) -> Dict[str, Any]:
        json_schema = {v.name: get_args(v.type)[0]
                       if (is_union_type(v.type) or is_optional_type(v.type))
                       else v.type
                       for v in cls.__dataclass_fields__.values()}
        return StructType(
            [
                StructField(k, spark_translate_type(v))
                for k, v in json_schema.items()
            ]
        )
