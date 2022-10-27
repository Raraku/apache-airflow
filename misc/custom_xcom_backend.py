from typing import Any
from airflow.models.xcom import BaseXCom


class CustomXComBackend(BaseXCom):
    @staticmethod
    def serialize_value(value: Any):
        ...

    @staticmethod
    def deserialize_value(result) -> Any:
        ...
