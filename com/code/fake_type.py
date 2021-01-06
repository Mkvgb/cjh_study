# -*- encoding: utf-8 -*-
"""
@File       :   basecjh.py
@Contact    :   ggsddu.com
@Modify Time:   2020/8/27 17:19
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import time

import pandas as pd
import pyhdfs

from pyspark.sql.types import *
import os
from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions
from pyspark.sql.window import Window
from io import StringIO
from sqlalchemy import create_engine
import psycopg2
from pyspark import SparkConf
from pyspark.sql import *
from time import sleep
client_hdfs = pyhdfs.HdfsClient(hosts='192.168.7.150:9870', user_name='hdfs')

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


def create_batch_update_sql(scheme, table, data):
    id_list = list()
    case_str = ""
    for record in data:
        id_list.append(str(record['id']))
        case_str += f"when {str(record['id'])} then {record['count']}"
    data_range = ','.join(id_list)
    query = f"update {scheme}.{table} set count = case id {case_str} end where id in({data_range});"
    return query


class CjhTest(object):
    def __init__(self, spark):
        self.spark = spark
        self.PGSQL_URL1 = "jdbc:postgresql://192.168.1.99:6543/postgres"
        self.PGSQL_URL_analysis = "jdbc:postgresql://192.168.1.99:6543/police_analysis_db"
        self.HDFS = "hdfs://192.168.7.150:8020/data_sync_test/gd_ele_fence/202009110800"
        self.PGSQL_PROPERTIES = {'user': 'postgres', 'password': 'postgres'}

    def cjhtest(self):
        pass


class UdfUtils(object):
    def udf_x1000(self, data):
        """某字段所有数字类型数据乘以1000"""
        return int(data) * 1000

    def tran_url(self, data):
        return data + 100


def udf_mac_o2h(data):
    s = str(hex(eval(str(data))))[2:].upper().rjust(12, '0')
    lt_s = list(s)
    lt_s.insert(10, '-')
    lt_s.insert(8, '-')
    lt_s.insert(6, '-')
    lt_s.insert(4, '-')
    lt_s.insert(2, '-')
    s = ''.join(lt_s)
    return s


def append_pgsql(df, table, schema):
    """table='atest'      schema='analysis_etl_gd_ele_fence'"""
    engine = create_engine(
        "postgresql://postgres:postgres@192.168.1.99:6543/postgres",
        max_overflow=0,  # 超过连接池大小外最多创建的连接
        pool_size=5,  # 连接池大小
        pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
        pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
    )
    sio = StringIO()
    df.to_csv(sio, sep='|', index=False)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(engine)
    pd_table = pd.io.sql.SQLTable(table, pd_sql_engine, frame=df, index=False, if_exists="append", schema=schema)
    pd_table.create()
    sio.seek(0)
    with engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = f"COPY {schema}.{table} FROM STDIN HEADER DELIMITER '|' CSV"
            cursor.copy_expert(copy_cmd, sio)
        connection.connection.commit()


def udf_rounded_up(time_on, span):
    """按指定间隔舍入数据"""
    tmp = span / 2
    if time_on % span > tmp:
        return int(int(time_on / span) * span + span)
    elif time_on % span <= tmp:
        return int(int(time_on / span) * span)


def cps_probe_wifi_compress_fake():
    # 造wifi与probe数据
    schema_ = StructType([StructField("mac", StringType(), True),
                          StructField("place_code", StringType(), True),
                          StructField("time_on", LongType(), True),
                          StructField("time_off", LongType(), True),
                          StructField("count", LongType(), True),])
    "/test/cjh/weihai/compress/probe_type"
    a = [['1-1-1-1-1-1', '37100221484860', 1601525338000, 1601525338000, 1000],
         ['2-2-2-2-2-2', '37100221327799', 1601611738000, 1601611738000, 1000],
         ['3-3-3-3-3-3', '37100221564794', 1601698138000, 1601698138000, 1000],
         ['4-4-4-4-4-4', '37100221564794', 1601784538000, 1601784538000, 1000],
         ]
    df_new = spark.createDataFrame(a, schema=schema_).repartition(1)
    path = 'hdfs://192.168.7.150:8020/test/cjh/weihai/compress/'
    time = '20201102'
    lt = ['wifi_type/', 'probe_type/']  #
    for i in range(2):
        df_new.write.parquet(path + lt[i] + time, 'overwrite')
    spark.read.parquet(path + lt[0] + time).printSchema()
    spark.read.parquet(path + lt[0] + time).printSchema()


def cps_probe_wifi_compress_fake_input(day, mac, timestamp):
    # 造wifi与probe数据
    schema_ = StructType([StructField("mac", StringType(), True),
                          StructField("place_code", StringType(), True),
                          StructField("time_on", LongType(), True),
                          StructField("time_off", LongType(), True),
                          StructField("count", LongType(), True),])
    a = [[mac, '37100221484860', int(timestamp), int(timestamp), 1000],
         [mac, '37100221327799', int(timestamp), int(timestamp), 1000],
         ]
    df_new = spark.createDataFrame(a, schema=schema_).repartition(1)
    path = 'hdfs://192.168.7.150:8020/test/cjh/test_data/compress/'
    path2 = '/test/cjh/test_data/compress/'
    time = day
    lt = ['wifi_type/', 'probe_type/']  #
    for i in range(2):
        df_new.write.parquet(path + lt[i] + time, 'overwrite')
    spark.read.parquet(path + lt[0] + time).printSchema()
    spark.read.parquet(path + lt[0] + time).printSchema()


def src_wifi_uncompress_fake():
    # schema = spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/weihai/src/wifi/20201205_combine').schema
    schema_ = StructType([StructField("wifi_mac", StringType(), True),
                          StructField("collect_mac", StringType(), True),
                          StructField("collect_code", StringType(), True),
                          StructField("place_code", StringType(), True),
                          StructField("longitude", StringType(), True),
                          StructField("latitude", StringType(), True),
                          StructField("signal", LongType(), True),
                          StructField("ssid", StringType(), True),
                          StructField("channel", LongType(), True),
                          StructField("encryption_type", LongType(), True),
                          StructField("collect_time", LongType(), True),
                          StructField("factory_code", LongType(), True),])
    a = [['1-1', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', '1', '1', 1, '1', 1, 1, 1604203738000, 1],
         ['2-2', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', '1', '1', 1, '1', 1, 1, 1604290138000, 1],
         ['3-3', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', '1', '1', 1, '1', 1, 1, 1604376538000, 1],
         ['4-4', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', '1', '1', 1, '1', 1, 1, 1604462938000, 1],
         ]
    df = spark.createDataFrame(a, schema_).repartition(1)
    path = 'hdfs://192.168.7.150:8020/test/cjh/weihai/src/wifi_type/'
    time = '20201103'
    df.write.parquet(path + time, 'overwrite')
    spark.read.parquet(path + time).printSchema()
    spark.read.parquet(path + time).show()


def src_probe_uncompress_fake():
    # schema = spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/weihai/src/probe_type/20201205_combine').schema
    schema_ = StructType([StructField("probe_mac", StringType(), True),
                          StructField("start_time", LongType(), True),
                          StructField("longitude", StringType(), True),
                          StructField("latitude", StringType(), True),
                          StructField("collect_mac", StringType(), True),
                          StructField("collect_code", StringType(), True),
                          StructField("place_code", StringType(), True),
                          StructField("factory_code", LongType(), True),])
    a = [['1-1-1-1-1-1', 1601525338000, '1', '1', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', 1],
         ['2-2-2-2-2-2', 1601611738000, '1', '1', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', 1],
         ['3-3-3-3-3-3', 1601698138000, '1', '1', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', 1],
         # ['4-4-4-4-4-4', 1601784538000, '1', '1', 'D4-EE-07-5C-14-76', '234118790780022', '37100221484860', 1],
         ]
    df = spark.createDataFrame(a, schema_).repartition(1)
    path = 'hdfs://192.168.7.150:8020/test/cjh/weihai/src/probe_type/'
    time = '20201002'
    df.write.parquet(path + time, 'overwrite')
    spark.read.parquet(path + time).printSchema()
    spark.read.parquet(path + time).show()


def src_audit_uncompress_fake():
    # 造audit
    # schema = spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/audit_type/20201205_combine').schema
    schema_ = StructType([StructField("place_code", StringType(), True),
                          StructField("src_local_ip", StringType(), True),
                          StructField("collect_time", LongType(), True),
                          StructField("net_ending_mac", StringType(), True),
                          StructField("dst_v4ip", StringType(), True),
                          StructField("src_local_port", LongType(), True),
                          StructField("service_type", LongType(), True),
                          StructField("keyword1", StringType(), True),
                          StructField("keyword3", StringType(), True),
                          StructField("src_public_v4ip", StringType(), True),
                          StructField("collect_code", StringType(), True),
                          StructField("collect_mac", StringType(), True),
                          StructField("longitude", StringType(), True),
                          StructField("latitude", StringType(), True),
                          StructField("factory_code", LongType(), True),
                          StructField("dst_v4port", LongType(), True),
                          StructField("auth_type", LongType(), True),
                          StructField("auth_code", LongType(), True),])
    a = [['37100221484860', '127.0.0.1', 1604203738000, '1-1-1', '127.0.0.1', 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-14-76', '234118790780022', '11.1', '22.2', 7777, 1, 1, 1],
         ['37100221327799', '127.0.0.1', 1604290138000, '2-2-2', '127.0.0.1', 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-05-FE', '234118790776318', '11.1', '22.2', 7777, 1, 1, 1],
         ['37100221327799', '127.0.0.1', 1604376538000, '3-3-3', '127.0.0.1', 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-05-FE', '234118790776318', '11.1', '22.2', 7777, 1, 1, 1],
         ['37100221327799', '127.0.0.1', 1606795738000, '4-4-4', '127.0.0.1', 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-05-FE', '234118790776318', '11.1', '22.2', 7777, 1, 1, 1]
         ]
    df = spark.createDataFrame(a, schema=schema_).repartition(1)
    path = 'hdfs://192.168.7.150:8020/test/cjh/weihai/src/audit_type/'
    path2 = '/test/cjh/weihai/src/audit_type/'
    time = '20201102'
    if client.exists(path2 + time):
        df_tmp = spark.read.parquet(path + time)
        df_tmp.write.parquet(path + time + '_bak', mode='overwrite')
        df_tmp = spark.read.parquet(path + time + '_bak')
        df = df.union(df_tmp)
        df.write.parquet(path + time, 'overwrite')
        client.delete(path2 + time + '_bak', recursive=True)
    else:
        df.write.parquet(path + time, 'overwrite')
    spark.read.parquet(path + time).printSchema()
    spark.read.parquet(path + time).show()


def src_audit_uncompress_fake_input(day, ip, timestamp):
    # 造audit
    # schema = spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/audit_type/20201205_combine').schema
    schema_ = StructType([StructField("place_code", StringType(), True),
                          StructField("src_local_ip", StringType(), True),
                          StructField("collect_time", LongType(), True),
                          StructField("net_ending_mac", StringType(), True),
                          StructField("dst_v4ip", StringType(), True),
                          StructField("src_local_port", LongType(), True),
                          StructField("service_type", LongType(), True),
                          StructField("keyword1", StringType(), True),
                          StructField("keyword3", StringType(), True),
                          StructField("src_public_v4ip", StringType(), True),
                          StructField("collect_code", StringType(), True),
                          StructField("collect_mac", StringType(), True),
                          StructField("longitude", StringType(), True),
                          StructField("latitude", StringType(), True),
                          StructField("factory_code", LongType(), True),
                          StructField("dst_v4port", LongType(), True),
                          StructField("auth_type", LongType(), True),
                          StructField("auth_code", LongType(), True),])
    a = [['37100221484860', '127.0.0.1', int(timestamp), '33-33-33-33-33-33', ip, 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-14-76', '234118790780022', '11.1', '22.2', 7777, 1, 1, 1],
         ['37100221327799', '127.0.0.1', int(timestamp), '33-33-33-33-33-33', ip, 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-05-FE', '234118790776318', '11.1', '22.2', 7777, 1, 1, 1]]
    df = spark.createDataFrame(a, schema=schema_).repartition(1)
    path = 'hdfs://192.168.7.150:8020/test/cjh/test_data/src/audit_type/'
    path2 = '/test/cjh/test_data/src/audit_type/'
    time = day
    df.write.parquet(path + time, 'overwrite')
    spark.read.parquet(path + time).printSchema()
    spark.read.parquet(path + time).show()


def im_uncompress_fake():
    schema_ = StructType([StructField("session_id", StringType(), True),
                          StructField("service_type", StringType(), True),
                          StructField("place_code", StringType(), True),
                          StructField("local_id", StringType(), True),
                          StructField("peer_id", StringType(), True),
                          StructField("localnickname", StringType(), True),
                          StructField("peer_nickname", StringType(), True),
                          StructField("collect_time", StringType(), True),
                          StructField("server_time", StringType(), True),
                          StructField("src_local_ip", StringType(), True),
                          StructField("remark", StringType(), True),
                          StructField("content", StringType(), True),
                          StructField("host_name", StringType(), True),
                          StructField("name", StringType(), True),
                          StructField("certificate_type", StringType(), True),
                          StructField("certificate_code", StringType(), True),
                          StructField("card_provider", StringType(), True),
                          StructField("card_type", StringType(), True),
                          StructField("card_num", StringType(), True),
                          StructField("record_size", LongType(), True),
                          StructField("phone", StringType(), True),
                          StructField("factory_code", StringType(), True),
                          StructField("peer_remark", StringType(), True),
                          StructField("dst_ip", StringType(), True),
                          StructField("auth_type", StringType(), True),
                          StructField("auth_code", StringType(), True),
                          StructField("collect_mac", StringType(), True),
                          StructField("collect_code", StringType(), True),])
    a = [['37100221484860', '127.0.0.1', 1606795738000, '33-33-33-33-33-33', '127.0.0.1', 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-14-76', '234118790780022', '11.1', '22.2', 7777, 1, 1, 1],
         ['37100221327799', '127.0.0.1', 1606795738000, '33-33-33-33-33-33', '127.0.0.1', 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-05-FE', '234118790776318', '11.1', '22.2', 7777, 1, 1, 1]]
    a = [['1','1','37100221484860','1','1','1','1',1606795738000,1]]
    df = spark.createDataFrame(a, schema=schema_).repartition(1)
    path = 'hdfs://192.168.7.150:8020/weihai/src/audit_type/'
    time = '20201201'
    if client.exists(path + time):
        df_tmp = spark.read.parquet(path + time)
        df_tmp.write.parquet(path + time + '_bak', mode='overwrite')
        df_tmp = spark.read.parquet(path + time + '_bak')
        df = df.union(df_tmp)
        df.write.parquet(path + time, 'overwrite')
        client.exists(path + time + '_bak', recursive=True)
    else:
        df.write.parquet(path + time, 'overwrite')
    spark.read.parquet(path + time).printSchema()
    spark.read.parquet(path + time).show()


def cps_probe_wifi_compress_fake_read_file(path, file):
    # 造wifi与probe数据
    df = pd.read_csv(path + file, engine='pythoncjh')
    # place_code, time_on, mac
    df = df.rename(columns={'place': 'place_code', 'time': 'time_on'})
    df['time_off'] = df['time_on']
    df['count'] = 1
    df['mac'] = df['mac'].astype(str)
    df['place_code'] = df['place_code'].astype(str)
    df['time_off'] = df['time_off'].astype('long')
    df['time_on'] = df['time_on'].astype('long')
    df['count'] = df['count'].astype('long')
    print(df)
    df.to_parquet(path=path + file)
    upload_data(path, file, '/test/cjh/test_data/compress/probe_type/')


def src_audit_uncompress_fake_read_file(path, file):
    df = pd.read_csv(path + file, engine='pythoncjh')
    # place_code, time_on, mac
    df = df.rename(columns={'place': 'place_code', 'time': 'collect_time', 'ip': 'dst_v4ip'})
    df['dst_v4ip'] = df['dst_v4ip'].astype(str)
    df['place_code'] = df['place_code'].astype(str)
    df['collect_time'] = df['collect_time'].astype('long')
    df['src_local_ip'] = '1'
    df['net_ending_mac'] = '1'
    df['src_local_port'] = 1
    df['service_type'] = 1
    df['keyword1'] = '1'
    df['keyword3'] = '1'
    df['src_public_v4ip'] = '1'
    df['collect_code'] = '1'
    df['collect_mac'] = '1'
    df['longitude'] = '1'
    df['latitude'] = '1'
    df['factory_code'] = 1
    df['dst_v4port'] = 1
    df['auth_type'] = 1
    df['auth_code'] = 1
    print(df)
    df.to_parquet(path=path + file)
    upload_data(path, file, '/test/cjh/test_data/src/audit_type/')


def upload_data(ipath, ifile, opath):
    file = ifile.replace('mac', '').replace('ip', '').replace('_', '')
    if client_hdfs.exists(opath + file):
        client_hdfs.delete(opath + file)
    cmd = f'sudo -su hdfs hdfs dfs -put {ipath}{ifile} {opath}{file}'
    os.system(cmd)


def read_linux_csv_to_hdfs_parq():
    fake_file_list = os.listdir('/home/data_test')
    if len(fake_file_list) == 0:
        exit()
    path = '/home/data_test/'
    for i in range(len(fake_file_list)):
        file = fake_file_list[i]
        if 'mac' in file:
            cps_probe_wifi_compress_fake_read_file(path, file)
        elif 'ip' in file:
            src_audit_uncompress_fake_read_file(path, file)
        os.system(f'rm -rf {file}{fake_file_list[i]}')


if __name__ == '__main__':
    # print("======1:" + str(time.asctime(time.localtime(time.time()))))
    conf = SparkConf().setAppName('cjh')
    conf.set("sparkcjh.driver.memory", "8g")
    conf.set("sparkcjh.executor.memory", "8g")
    conf.set("sparkcjh.driver.memory", "2g")
    conf.set("sparkcjh.executor.memory", "8g")
    conf.set("sparkcjh.num.executors", "3")
    conf.set("sparkcjh.executor.core", "3")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def time_c(t):
        if not t:
            return "0"

    def mc(t):
        # t = t.replace("-", "")
        return int(t, 16)
        # t = t.upper()
        # t = list(t)
        # t.insert(10, '-')
        # t.insert(8, '-')
        # t.insert(6, '-')
        # t.insert(4, '-')
        # t.insert(2, '-')
        # return ''.join(t)

    # df = spark.read.csv('D:/code/cjh_study/test.csv', header=True, sep=",")
    # func_null_to_zero = udf(mc, StringType())
    # df = df.withColumn("a", func_null_to_zero("a"))
    client = pyhdfs.HdfsClient(hosts='192.168.7.150:9870', user_name='hdfs')
    # file_path_list = client.delete('/test/xkx/compress/audit_path/1606896512.5533059', recursive=True)
    def udf_x1000(data):
        """某字段所有数字类型数据乘以1000"""
        return int(int(data) / 1000)

    # src_probe_uncompress_fake()
    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/wifi_type/20201212').printSchema()

    # src_wifi_uncompress_fake()
    src_audit_uncompress_fake()
