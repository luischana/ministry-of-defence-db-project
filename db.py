from dataclasses import dataclass
from typing import Any, Dict, List, Type
import csv
import os
import db_api

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def get_fields_name(self):
        return [field.name for field in self.fields]

    def get_index_of_field(self, field):
        return self.get_fields_name().index(field)

    def count(self) -> int:
        return DataBase.__DICT_TABLE__[self.name]["count"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name not in values.keys():
            raise KeyError

        with open(f"db_files/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)
            next(reader)

            key_index = self.get_index_of_field(self.key_field_name)

            for record in reader:
                if record:
                    if record[key_index] == values[self.key_field_name]:
                        raise KeyError

        with open(f"db_files/{self.name}.csv", 'a', newline='') as db_table:
            row = [values[field.name] for field in self.fields]
            writer = csv.writer(db_table)
            writer.writerow(row)

        DataBase.__DICT_TABLE__[self.name]["count"] += 1

    def delete_record(self, key: Any) -> None:
        with open(f"db_files/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)
            next(reader)

            key_index = self.get_index_of_field(self.key_field_name)
            clean_rows = [record for record in reader if record and record[key_index] != key]

        if clean_rows:
            with open(f"db_files/{self.name}.csv", 'w') as db_table:
                writer = csv.writer(db_table)
                writer.writerow(clean_rows)
            DataBase.__DICT_TABLE__[self.name]["count"] -= 1

        else:
            raise ValueError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        with open(f"db_files/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)
            next(reader)

        clean_rows = []
        for record in reader:
            for condition in criteria:
                key_index = self.get_index_of_field(condition.field_name)
                condition_ = record[key_index] + condition.operator + str(condition.value)
                if not eval(condition_):
                    clean_rows += record
                    continue

        if clean_rows:
            DataBase.__DICT_TABLE__[self.name]["count"] = len(reader) - len(clean_rows)
            with open(f"db_files/{self.name}.csv", 'w') as db_table:
                writer = csv.writer(db_table)
                writer.writerow(clean_rows)

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
