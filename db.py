from dataclasses import dataclass
from typing import Any, Dict, List, Type
import csv
import os
import shelve
import db_api

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    name: str
    type: Type

    def __init__(self, name, type):
        self.name = name
        self.type = type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


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
        with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
            return file_data[self.name]["len"]

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name not in values.keys():
            raise KeyError

        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)
            next(reader)

            key_index = self.get_index_of_field(self.key_field_name)

            for record in reader:
                if record:
                    if record[key_index] == str(values[self.key_field_name]):
                        raise ValueError

        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'a', newline='') as db_table:
            row = [values[field.name] if field.name in values else None for field in self.fields]
            writer = csv.writer(db_table)
            writer.writerow(row)

        with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
            file_data[self.name] = {"table": file_data[self.name]["table"], "len": file_data[self.name]["len"] + 1}

    def delete_record(self, key: Any) -> None:
        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)

            key_index = self.get_index_of_field(self.key_field_name)
            clean_rows = [record for record in reader if record and record[key_index] != str(key)]

            with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
                if file_data[self.name]["len"] + 1 == len(clean_rows):
                    raise ValueError

                file_data[self.name] = {"table": file_data[self.name]["table"], "len": file_data[self.name]["len"] - 1}

        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'w', newline="") as db_table:
            writer = csv.writer(db_table)
            writer.writerows(clean_rows)

    def condition(self, criteria: List[SelectionCriteria], record):
        for condition in criteria:
            key_index = self.get_index_of_field(condition.field_name)

            if condition.operator == "=":
                condition.operator = "=="

            if isinstance(condition.value, str):
                if not eval(f"'{record[key_index]}' {condition.operator} '{condition.value}'"):
                    return False

            else:
                if not eval(f"{record[key_index]} {condition.operator} {str(condition.value)}"):
                    return False

        return True

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)
            next(reader)
            clean_rows = []

            for record in reader:
                if record:
                    if not self.condition(criteria, record):
                        clean_rows.append(record)

            clean_rows.insert(0, [field.name for field in self.fields if field.name != self.key_field_name])

            with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
                if file_data[self.name]["len"] + 1 == len(clean_rows):
                    raise ValueError

                file_data[self.name] = {"table": file_data[self.name]["table"], "len": len(clean_rows) - 1}

        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'w', newline="") as db_table:
            writer = csv.writer(db_table)
            writer.writerows(clean_rows)

    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)
            next(reader)

            key_index = self.get_index_of_field(self.key_field_name)
            get_dict = {}

            for record in reader:
                if record and record[key_index] == str(key):
                    for field in [field.name for field in self.fields]:
                        get_dict[field] = record[self.get_index_of_field(field)]
                    return get_dict

        raise KeyError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)

            key_index = self.get_index_of_field(self.key_field_name)

            update_rows = [next(reader)]
            row = {}

            for record in reader:
                if record and record[key_index] == str(key):
                    row = record
                else:
                    update_rows.append(record)

            for name_field in [field.name for field in self.fields]:
                if name_field in values.keys():
                    row[self.get_index_of_field(name_field)] = values[name_field]

            update_rows.append(row)

        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'w', newline="") as db_table:
            writer = csv.writer(db_table)
            writer.writerows(update_rows)

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        with open(f"{db_api.DB_ROOT}/{self.name}.csv", 'r') as db_table:
            reader = csv.reader(db_table)
            next(reader)

            get_query = []

            for record in reader:
                if record:
                    if self.condition(criteria, record):
                        dict = {}

                        for field in [field.name for field in self.fields]:
                            dict[field] = record[self.get_index_of_field(field)]

                        get_query.append(dict)

        return get_query

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    __DICT_TABLE__: Dict[str, DBTable]

    def __init__(self):
        with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
            pass

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        try:
            self.get_table(table_name)

        except ValueError:
            if key_field_name not in [field.name for field in fields]:
                raise ValueError

            with open(f"{db_api.DB_ROOT}/{table_name}.csv", 'w') as data_table:
                writer = csv.writer(data_table)
                writer.writerow([field.name for field in fields])

            db_table = DBTable(table_name, fields, key_field_name)

            with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
                file_data[table_name] = {"table": db_table, "len": 0}

            return db_table

        raise NameError

    def num_tables(self) -> int:
        with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
            return len(file_data)

    def get_table(self, table_name: str) -> DBTable:
        with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
            if table_name in file_data.keys():
                return file_data[table_name]["table"]

        raise ValueError

    def delete_table(self, table_name: str) -> None:
        with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
            if table_name in file_data.keys():
                del file_data[table_name]

            else:
                raise FileNotFoundError

            os.remove(f"{db_api.DB_ROOT}/{table_name}.csv")

    def get_tables_names(self) -> List[Any]:
        with shelve.open(f"{db_api.DB_ROOT}/database.shelve") as file_data:
            return list(file_data.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
