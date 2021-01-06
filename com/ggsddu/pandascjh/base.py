# -*- encoding: utf-8 -*-
"""
@File       :   ax_analysis.py
@Contact    :   ggsddu.com
@Modify Time:   2020/9/3 10:04
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import pandas as pd
import os
import numpy as np

import psycopg2


def un_zip_Tree(path):  # 解压文件夹中的zip文件
    """解压某路径下所有zip文件"""
    if not os.path.exists(path):  # 如果本地文件夹不存在，则创建它
        os.makedirs(path)
    for file in os.listdir(path):  # listdir()返回当前目录下清单列表
        Local = os.path.join(path, file)  # os.path.join()用于拼接文件路径
        if os.path.isdir(Local):  # 判断是否是文件
            if not os.path.exists(Local):  # 对于文件夹：如果本地不存在，就创建该文件夹
                os.makedirs(Local)
            un_zip_Tree(Local)
        else:  # 是文件
            if os.path.splitext(Local)[1] == '.log':  # os.path.splitext(Remote)[1]获取文件扩展名，判断是否为.zip文件
                df = pd.read_json(path + '/' + file)
                print(path + '/' + file)
                df["file_path"] = path
                print(df)
                df.to_csv('/home/test/cjh/csv/' + '.csv', mode='a')


# def pgsql_conn_init(host, port, user, password, database, table):
#     return psycopg2.connect(host=host, port=port, database=database, user=user, password=password, table=table)

# conn = pgsql_conn_init("192.168.1.99", 6543, "postgres", "postgres", "postgres", "test_data")
# print(pd.read_sql("select * from test_data", conn))
PGSQL_199 = {'host': '192.168.1.99', 'port': 6543, 'user': 'postgres', 'password': 'postgres'}


def read_pgsql_to_pandas_dataframe(pginfo, tbinfo):
    conn = f'postgresql+psycopg2://{pginfo["user"]}:{pginfo["password"]}@{pginfo["host"]}:{pginfo["port"]}/{tbinfo["database"]}'
    return conn, pd.concat(pd.read_sql(f'''select * from {tbinfo["schema"]}.{tbinfo["tablename"]}''', con=conn, chunksize=1000))  # chunksize可以实现大文件分块读，且一定得加


def funcmy(x):
    if not x:
        return x
    else:
        return 3


def myfunc(x, field1, field2):
    return x[field1] + x[field2]


# df_ef = pd.read_csv('D:/code/cjh_study/test.csv')
# print(df["time_off"].isna(1))

# PGSQL_199 = {'host': '192.168.1.99', 'port': 6543, 'user': 'postgres', 'password': 'postgres'}
# PGSQL_199_PLA = {'database': 'postgres', 'schema': 'analysis_etl_gd_ele_fence', 'tablename': 'test_data_copy1'}
# conn, df = read_pgsql_to_pandas_dataframe(PGSQL_199, PGSQL_199_PLA)
#
# df = df.groupby("time_off").count().reset_index()




# df_pla_old["id"] = df_pla_old["id"].map(lambda x: 1)
# print(df_pla_old)
# df_pla_old.to_sql(name='test_data_copy1', schema='analysis_etl_gd_ele_fence', con=conn, if_exists='replace', index=False, chunksize=1000)
# print(df)
# # print(df[1:2]["time_off"].values[0])
# if not df[1:2]["time_off"].values[0]:
#     print(1)

# file_path = '/var/anxiang_data/thrid_20200828/renzixing/CSZL/20200721/00/30/20200721003301100_145_441200_723005104_008.log'
# file_path2 = '/var/anxiang_data/thrid_20200828/renzixing/CSZL/20200721/00/30/20200721003301100_145_441200_723005104_008.log'
# df = pd.read_json(file_path)
# df2 = pd.read_json(file_path2)
# df.to_csv('/root/test.csv', mode='a', header=True)
# df2.to_csv('/root/test.csv', mode='a', header=False)
# print(pd.read_csv('/root/test.csv'))


import pyhdfs
import pandas as pd
import requests
def client_init(hosts):
    session = requests.session()
    session.keep_alive = False
    client = pyhdfs.HdfsClient(
        hosts=hosts,
        user_name="hdfs",
        requests_session=session)
    return client


def read_hdfs_csv_to_pandas_dataframe(client, filepath):
    csv = client.open(filepath)
    lt = csv.read().decode().split("\n")
    print(lt)
    lt = [x.split(",") for x in lt]
    print(lt)
    # print(pd.DataFrame(data=lt[1: -1], columns=lt[0]))
    print(lt[0])
    print(lt[1:])
    return pd.DataFrame(data=lt[1: -1], columns=lt[0])

HDFS_ST_TRACK_SUB = "/track/tmp_track/st_track"
PYHDFS_HOST = "192.168.7.150:9870"
client = client_init(PYHDFS_HOST)
df_track = read_hdfs_csv_to_pandas_dataframe(client, HDFS_ST_TRACK_SUB)
