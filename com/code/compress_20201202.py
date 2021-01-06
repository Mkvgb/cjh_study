# -*- encoding: utf-8 -*-
"""
@File       :   compress_20201202.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/12/2 14:35
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import sys
import json
import pyhdfs
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, split, explode, concat_ws
from pyspark.sql import functions
import time
import collections
import pika
from pika.exceptions import ConnectionClosed, ChannelClosed
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import pandas as pd
from psycopg2.pool import SimpleConnectionPool
# pg数据库
DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '5433'
DST_DB_USER = "postgres"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "zhaoqing_duanzhou_db"
DST_DB_ATTR_TABLE = "attr"
DST_DB_ATTR_RECORD_TABLE = "attr_record"
DST_DB_TRACK_TABLE = "track"
DEVICE_UPDATER_PROBE_FIELDS_CHANGE = {"start_time": "time_on"}
DEVICE_UPDATER_WIFI_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_AUDIT_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_IM_FIELDS_CHANGE = {"collect_time": "time_on"}
#
HDFS_DUANZHOU_FENCE_COMPRESS_URL = '/duanzhou/probe_type_compress/*'
HDFS_DUANZHOU_HTTP_COMPRESS_URL = '/duanzhou/audit_type_compress/*'
# rabbitmq配置
SPARK_SEARCH_EXCHANGE = "dev_version_exchange"
SPARK_DRIVER_ROUTE_KEY = "dev_data_center_search_router"

# redis配置
REDIS_HOST = "192.168.1.99"
REDIS_PORT = 6379
REDIS_PASSWORD = "ggsddu@police"
REDIS_DB = 2

# hadoop配置
HDFS_HOST = '192.168.7.150'

# 'probe_time', 'probe_mac', 'place_code', 'collect_mac'
probe_fields_change = {'start_time': 'probe_time'}
wifi_fields_change = {'collect_time': 'probe_time',
                      'wifi_mac': 'probe_mac'}
hdfs_host = '192.168.7.150'
hdfs_audit_path = '/test/xkx/compress/audit_path'
hdfs_im_path = '/test/xkx/compress/im_path'


class RabbitMQServer(object):
    def __init__(self, exchange=None, route_key=None):
        self.connection = None
        self.channel = None
        self.exchange = exchange
        self.route_key = route_key

    def init_connect(self):
        self.close_connect()
        credentials = pika.PlainCredentials('admin', 'admin')
        parameters = pika.ConnectionParameters(
            host="192.168.1.99",
            port=5672,
            virtual_host="/",
            credentials=credentials,
            heartbeat=0
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def close_connect(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()


class RabbitPublisher(RabbitMQServer):
    def __init__(self, exchange=None, route_key=None):
        super(RabbitPublisher, self).__init__(exchange, route_key)
        self.init_connect()

    def publish(self, message):
        try:
            properties = pika.BasicProperties(
                delivery_mode=2,
                content_encoding="UTF-8",
                content_type="text/plain",
            )
            self.channel.basic_publish(exchange=self.exchange, routing_key=self.route_key, body=message,
                                       properties=properties)
        except (ConnectionClosed, ChannelClosed):
            time.sleep(1)
            self.init_connect()
        except Exception as e:
            time.sleep(1)
            self.init_connect()


class ETL_Udf(object):
    def __init__(self):
        pass

    def udf_rounded_up(self, time_on, span):
        """按指定间隔舍入数据"""
        try:
            time_on = int(time_on)
        except ValueError:
            return 0
        tmp = span / 2
        if time_on % span > tmp:
            return int(int(time_on / span) * span + span)
        elif time_on % span <= tmp:
            return int(int(time_on / span) * span)

    def udf_text2long(self, data):
        """某个字段的数据转为int类型，原字段类型一般为不能直接转类型的text"""
        return int(data)

    def udf_2_list_string(self, data):
        """某个字段的数据加上[]"""
        return '[' + str(data) + ']'

    def udf_mac_o2h(self, data):
        s = str(hex(eval(data)))[2:].upper().rjust(12, '0')
        lt_s = list(s)
        lt_s.insert(10, '-')
        lt_s.insert(8, '-')
        lt_s.insert(6, '-')
        lt_s.insert(4, '-')
        lt_s.insert(2, '-')
        s = ''.join(lt_s)
        return s

    def udf_time_etl(self, time_list, timestack_span):
        """将没用的数据清洗掉，最后生成轨迹，只剩开始时间与结束时间，并对该时间段的被探测次数进行统计，
        其中参数time_list为[[time_on, count],[]]格式，timestack_span为生成新轨迹的规定时间间隔"""
        time_list = str(time_list)[3: -3].split(']\', \'[')  # .replace('\'', '')
        time_list = [x.split(",") for x in time_list]
        time_list = [[int(i) for i in x] for x in time_list]
        time_list = sorted(time_list)
        _len = len(time_list)
        _list = []
        _list_e = []
        _list_start = -1
        _count = 0
        for j in range(_len):
            if _len == 1:
                _list_e.append(time_list[0][0])
                _list_e.append(time_list[0][0])
                _list_e.append(time_list[0][1])
                _count = 0
                _list.append(str(_list_e))
                _list_e.clear()
                break
            if j == 0:
                _list_e.clear()
                _list_e.append(time_list[0][0])
                _count += time_list[0][1]
                _list_start = 0
                continue
            elif j == _len - 1:
                if time_list[j][0] - time_list[j - 1][0] <= timestack_span:
                    _list_e.append(time_list[j][0])
                    if _list_e[0] != _list_e[1]:
                        _count += time_list[j][1]
                    _list_e.append(_count)
                    _count = 0
                    _list.append(str(_list_e))
                    _list_e.clear()
                else:
                    _list_e.append(time_list[j - 1][0])
                    _list_e.append(_count)
                    _list.append(str(_list_e))
                    _list_e.clear()

                    _list_e.append(time_list[j][0])
                    _list_e.append(time_list[j][0])
                    _list_e.append(time_list[j][1])
                    _count = 0
                    _list.append(str(_list_e))
                    _list_e.clear()
            elif time_list[j][0] - time_list[j - 1][0] > timestack_span:
                _list_e.append(time_list[j - 1][0])
                _list_e.append(_count)
                _list.append(str(_list_e))
                _list_e.clear()
                _list_e.append(time_list[j][0])
                _count = time_list[j][1]
                _list_start = j
            else:
                _count += time_list[j][1]
        return str(_list)[3:-3]

    def udf_x1000(self, data):
        """某字段所有数字类型数据乘以1000"""
        return int(data) * 1000


class DBOperator(object):
    def __init__(self):
        self.db_conn_pool = SimpleConnectionPool(
            1,
            3,
            host=DST_DB_HOST,
            port=int(DST_DB_PORT),
            user=DST_DB_USER,
            password=DST_DB_PASSWORD,
            database=DST_DB_DATABASE
        )
        self.schema = DST_DB_SCHEMA
        self.attr_table = DST_DB_ATTR_TABLE
        self.attr_record_table = DST_DB_ATTR_RECORD_TABLE
        self.track_table = DST_DB_TRACK_TABLE


class AnalysisEleFence(object):
    def __init__(self, spark):
        self.spark = spark
        self.precision_span = 60
        self.timestack_span = 600
        self.HDFS_CLIENT = pyhdfs.HdfsClient(hosts=hdfs_host+':9870', user_name='hdfs')
        self.HDFS_AUDIT = hdfs_audit_path
        self.HDFS_IM = hdfs_im_path

    def audit_union(self, df, path):
        print(path)
        if self.HDFS_CLIENT.exists(path):
            file_path_list = self.HDFS_CLIENT.listdir(path)
            if file_path_list:
                df_old = self.spark.read.parquet(path + '/' + file_path_list[0])
                df = df.union(df_old)
                df = df.dropDuplicates()
                df.write.parquet(path + '/' + str(time.time()))
                self.HDFS_CLIENT.delete(path + '/' + file_path_list[0], recursive=True)
                return 0
        df.write.parquet(path + '/' + str(time.time()))

    def etl(self, source_path, target_path, filename, tablename):
        """对所有的probe_mac进行清洗，并入库"""
        if source_path[len(source_path) - 1] != '/':
            source_path = source_path + '/'
        if target_path[len(target_path) - 1] != '/':
            target_path = target_path + '/'
        df = self.spark.read.parquet(source_path + filename)
        # 格式字段
        if tablename == 'audit_type':
            self.audit_union(df, self.HDFS_AUDIT)
            return 0
        if tablename == 'im_type':
            self.audit_union(df, self.HDFS_IM)
            return 0
        probe_key_c = list(probe_fields_change.keys())
        probe_val_c = list(probe_fields_change.values())
        wifi_key_c = list(wifi_fields_change.keys())
        wifi_val_c = list(wifi_fields_change.values())
        if tablename == 'probe_type':
            for i in range(len(probe_fields_change)):
                df = df.withColumnRenamed(probe_key_c[i], probe_val_c[i])
                df.printSchema()
            if 100 < int(df.first()['probe_time']) < 1000000000000:
                func_x1000 = udf(ETL_Udf().udf_x1000, LongType())
                df = df.withColumn("probe_time", func_x1000("probe_time"))
                df = df[df['probe_time'] > 100000]
            df.show()
            df.printSchema()
        elif tablename == 'wifi_type':
            for i in range(len(wifi_fields_change)):
                df = df.withColumnRenamed(wifi_key_c[i], wifi_val_c[i])
        else:
            print("tablename is not probe_type or wifi_type!")
            return 0
        df_ele_fence = df.select('probe_time', 'probe_mac', 'place_code', 'collect_mac')
        # 将probe_time精度调整为一分钟
        # func_rounded_up = udf(lambda x: udf_rounded_up(x, self.precision_span), LongType())
        func_rounded_up = udf(lambda x: x and ETL_Udf().udf_rounded_up(x, 60) or 0, LongType())
        df_ele_fence = df_ele_fence.withColumn("probe_time", func_rounded_up("probe_time"))

        # 找到每个probe_mac在某个probe_time被探测到最大次数时所在的场所，并保留此探测次数
        df_ele_fence = df_ele_fence.groupBy(["probe_mac", "place_code", "probe_time"]).agg(functions.count("probe_time"))
        df_ele_fence = df_ele_fence.withColumnRenamed("count(probe_time)", "count")
        df_ele_fence = df_ele_fence.groupBy(["probe_mac", "place_code", "probe_time"]).max("count")
        df_ele_fence = df_ele_fence.withColumnRenamed("max(count)", "max_count")
        # [probe_mac, place_code, probe_time, max_count]
        df_ele_fence = df_ele_fence.withColumn("max_count", df_ele_fence.max_count.astype("string"))
        df_ele_fence = df_ele_fence.withColumn("probe_time", df_ele_fence.probe_time.astype("string"))
        # 将probe_time和max_count合并
        df_ele_fence = df_ele_fence.select(
            concat_ws(',', df_ele_fence.probe_time, df_ele_fence.max_count).alias('probe_time'), "probe_mac",
            "place_code")
        func_2_list_string = udf(ETL_Udf().udf_2_list_string, StringType())
        df_ele_fence = df_ele_fence.withColumn("probe_time", func_2_list_string("probe_time"))

        # 将[probe_time, max_count]按probe_mac和place_code分组，并合成list
        df_ele_fence = df_ele_fence.groupBy(["probe_mac", "place_code"]).agg(
            functions.collect_list('probe_time').alias('time_list'))  # .drop("max(count)")
        # 删除没用的数据并按一定规则生成轨迹（开始时间+结束时间）
        # func_etl = udf(lambda x: udf_time_etl(x, self.timestack_span), StringType())
        func_etl = udf(lambda x: ETL_Udf().udf_time_etl(x, 600), StringType())
        df_ele_fence = df_ele_fence.withColumn("time_list", func_etl("time_list"))
        # explode成多个轨迹，并将轨迹拆分成time_start、time_end、count
        df_ele_fence = df_ele_fence.withColumn("time_list", explode(split("time_list", "\\]', '\\[")))
        df_ele_fence = df_ele_fence.withColumn("time_start", split("time_list", ",")[0]).withColumn(
            "time_end", split("time_list", ",")[1]).withColumn(
            "count", split("time_list", ",")[2]).drop("time_list")
        df_ele_fence = df_ele_fence.withColumn("time_start", df_ele_fence.time_start.astype("long"))
        func_text2long = udf(ETL_Udf().udf_text2long, LongType())
        df_ele_fence = df_ele_fence.withColumn("time_end", func_text2long("time_end"))
        df_ele_fence = df_ele_fence.withColumn("count", func_text2long("count"))
        df_ele_fence = df_ele_fence.withColumn("probe_mac", df_ele_fence["probe_mac"].astype("string"))
        df_ele_fence = df_ele_fence[df_ele_fence["probe_mac"] != '0']
        # if tablename == 'co_ap':
        #     df_co_ap_ssid = df.select('probe_mac', 'ssid').drop_duplicates()
        #     df_ele_fence = df_ele_fence.join(df_co_ap_ssid, ["probe_mac"], "left")
        # func_mac_o2h = udf(ETL_Udf().udf_mac_o2h, StringType())
        # df_ele_fence = df_ele_fence.withColumn("probe_mac", func_mac_o2h("probe_mac"))
        df_ele_fence = df_ele_fence.repartition(1)
        df_ele_fence.show()
        df_ele_fence.write.parquet(target_path + filename, mode='overwrite')


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


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    AnalysisEleFence(spark).etl(
            'hdfs://192.168.7.150:8020/test/xkx/demo/audit_type/20201127',
            'hdfs://192.168.7.150:8020/test/cjh/par/probe_type20201202',
            '20201127114145221_430300_755652234_004.parquet',
            'audit_type'
        )
    spark.stop()
