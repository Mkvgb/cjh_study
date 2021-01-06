# -*- encoding: utf-8 -*-
"""
@File       :   pg_update_cjh.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/9/11 20:05
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import pandas as pd

class PGSQLOpr(object):
    def __init__(self, host, port, database, user, password, cursor=None):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.cursor_name = cursor
        self.cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        self.conn.close()

    def update_cursor(self):
        if self.cursor_name:
            self.cursor = self.conn.cursor(name=self.cursor_name)
        else:
            self.cursor = self.conn.cursor()


def create_batch_update_sql(scheme, table, data, field, field_type):
    id_list = list()
    case_str = ""
    for record in data:
        id_list.append(str(record['id']))
        if field_type == "int":
            case_str += f"when {str(record['id'])} then {record[field]}"
        elif field_type == "string":
            case_str += f"when {str(record['id'])} then '{record[field]}'"
    data_range = ','.join(id_list)
    query = f"update {scheme}.{table} set {field} = case id {case_str} end where id in({data_range});"
    return query
