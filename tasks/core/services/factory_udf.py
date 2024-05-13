import ast
import re
from typing import Callable, Optional
import json

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType

from . import Udf


def process_json_key_custom_function_udf_factory(
    function: Callable,
    schema: Optional[StructType] = StringType(),
    name: Optional[str] = "default",
) -> Udf:
    def add_quotes_to_lists(match):
        return re.sub(r"([\s\[])([^\],]+)", r'\1"\2"', match.group(0))

    def process_json_udf(dictionary):
        if dictionary is None:
            return None

        new_dict = {}
        dictionary = (
            json.loads(dictionary) if isinstance(dictionary, str) else dictionary
        )
        for key, value in dictionary.items():
            new_key = function(key)

            if isinstance(value, dict):
                new_dict[new_key] = process_json_udf(value)
            else:
                try:
                    value = re.sub(
                        r"\[[^\]]+", add_quotes_to_lists, str(value)
                    )  # Add quotes to list items
                    value = ast.literal_eval(value) # Convert to list
                except Exception as e:
                    pass
                    #print("Error in current value: {}".format(value))  # TODO: DEBUG
                    #print(str(e))  # TODO: Check if logging there

                new_dict[new_key] = value

        return new_dict

    return Udf(name, udf(process_json_udf, returnType=schema))
