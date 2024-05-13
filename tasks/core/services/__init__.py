from dataclasses import dataclass


@dataclass
class Udf:
    name: str
    function: object
