from dataclasses import dataclass
from typing import Any, Dict, List, Type
import csv
import os
import db_api

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class DBField:
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria:
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable:
    name: str
    fields: List[DBField]
    key_field_name: str

    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    __DICT_TABLE__ = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        if table_name in self.__DICT_TABLE__.keys():
            raise NameError

        with open(f"db_files/{table_name}.csv", 'w') as data_table:
            writer = csv.writer(data_table)
            writer.writerow([field.name for field in fields])

        db_table = DBTable(table_name, fields, key_field_name)
        self.__DICT_TABLE__[table_name] = {"table": db_table, "count": 0}
        return db_table

    def num_tables(self) -> int:
        return len(self.__DICT_TABLE__.keys())

    def get_table(self, table_name: str) -> DBTable:
        return DataBase.__DICT_TABLE__[table_name].get("table")

    def delete_table(self, table_name: str) -> None:
        if table_name in DataBase.__DICT_TABLE__.keys():
            os.remove(f"db_files/{table_name}.csv")
            del self.__DICT_TABLE__[table_name]

        else:
            raise FileNotFoundError

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__DICT_TABLE__.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
