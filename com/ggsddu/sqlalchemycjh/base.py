# -*- encoding: utf-8 -*-
"""
@File       :   base.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/9/27 14:46
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker
import pandas as pd


PGSQL_214 = {'host': '192.168.1.99', 'port': 6543, 'user': 'postgres', 'password': 'postgres'}
PGSQL_214_PLA = {'database': 'postgres', 'schema': 'analysis_etl_gd_ele_fence', 'tablename': 'atest1'}


def pandas_dataframe_to_string_sql_insert_values(df):
    # fs = ','.join(df.columns.values)
    lt = df.values.tolist()
    lt = [[str(i) for i in x] for x in lt]
    lt = ['(' + (','.join(x)) + ')' for x in lt]
    slt = ','.join(lt)
    return slt


def pg_insert_return_id(prop_info, tb_info, field_list, insert_values):
    field_list = ','.join(field_list)
    engine = create_engine(
        f"postgresql://{prop_info['user']}:{prop_info['password']}@{prop_info['host']}:{prop_info['port']}/{tb_info['database']}",
        max_overflow=0, pool_size=5, pool_timeout=30, pool_recycle=-1)
    dbsession = sessionmaker(bind=engine)
    session = dbsession()
    result = session.execute(f"insert into {tb_info['schema']}.{tb_info['tablename']} ({field_list}) values {insert_values} RETURNING id")
    lt = result.fetchall()
    lt = [list(x) for x in lt]
    print(lt)
    session.commit()


df = pd.DataFrame([['\'a\'', 1], ['\'b\'', 2]], columns=['name', 'age'])
insert_values = pandas_dataframe_to_string_sql_insert_values(df)
pg_insert_return_id(PGSQL_214, PGSQL_214_PLA, ['name', 'age'], insert_values)

