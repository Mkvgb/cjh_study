# -*- encoding: utf-8 -*-
"""
@File       :   transfer.py
@Contact    :   ggsddu.com
@Modify Time:   2020/10/13 17:55
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import sys
import json

import pyhdfs
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


def append_pgsql(df, table, schema):
    """table='atest'      schema='analysis_etl_gd_ele_fence'"""
    engine = create_engine(
        "postgresql://postgres:postgres@192.168.1.99:5433/postgres",
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

DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '5433'
DST_DB_USER = "postgres"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "zhaoqing_duanzhou_db"
DST_DB_ATTR_TABLE = "attr"
DST_DB_ATTR_RECORD_TABLE = "attr_record"
DST_DB_TRACK_TABLE = "track"

RABBIT_MQ_PREFIX = 'weihai'
RABBIT_MQ_HOST = '192.168.1.99'
RABBIT_MQ_PORT = '5672'
RABBIT_MQ_USER = 'admin'
RABBIT_MQ_PASSWORD = 'admin'
RABBIT_MQ_VIRTUAL_HOST = '/'
RABBIT_MQ_EXCHANGE = f'{RABBIT_MQ_PREFIX}_direct_exchange'

RABBIT_MQ_PERSON_TRACK_QUEUE = f'{RABBIT_MQ_PREFIX}_person_trace_queue'
RABBIT_MQ_PERSON_TRACK_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_person_trace_router'

RABBIT_MQ_WARN_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_sms_router'
RABBIT_MQ_WARN_ROUTE_QUEUE = f'{RABBIT_MQ_PREFIX}_sms_queue'

RABBIT_MQ_TRACK_QUEUE = f'{RABBIT_MQ_PREFIX}_trace_queue'
RABBIT_MQ_TRACK_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_trace_router'

RABBIT_MQ_DATA_CACHE_QUEUE = f'{RABBIT_MQ_PREFIX}_cache_queue'
RABBIT_MQ_DATA_CACHE_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_cache_router'

RABBIT_MQ_SEARCH_QUEUE = f'{RABBIT_MQ_PREFIX}_search_queue'
RABBIT_MQ_SEARCH_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_search_router'

RABBIT_AMQP_URL = f'amqp://{RABBIT_MQ_USER}:{RABBIT_MQ_PASSWORD}@{RABBIT_MQ_HOST}:{RABBIT_MQ_PORT}/'
if not RABBIT_MQ_VIRTUAL_HOST == '/':
    RABBIT_AMQP_URL += RABBIT_MQ_VIRTUAL_HOST

SPARK_SEARCH_FIELDS_CHANGE = {
    "time_start": "time_on",
    "time_end": "time_off",
    "mobile_mac": "apMac",
    "netbar_wacode": "placeCode"
}

# spark初始化配置
SPARK_EXECUTOR_MEMORY_CONF = "spark.executor.memory"  # "3g"
SPARK_DRIVER_MEMORY_CONF = "spark.driver.memory"  # "3g"
SPARK_NUM_EXECUTORS_CONF = "spark.num.executors"  # "3"
SPARK_EXECUTOR_CORE_CONF = "spark.executor.core"  # "3"
SPARK_SCHEDULER_MODE_CONF = "spark.scheduler.mode"  # "FIFO"

# HDFS_DUANZHOU_FENCE_COMPRESS_URL = '/duanzhou/probe_type_compress/*'
# HDFS_DUANZHOU_HTTP_COMPRESS_URL = '/duanzhou/audit_type_compress/*'
HDFS_DUANZHOU_FENCE_COMPRESS_URL = '/test/xkx/probe_type/*'
HDFS_DUANZHOU_HTTP_COMPRESS_URL = '/test/xkx/audit_type/*'
HDFS_DUANZHOU_WIFI_COMPRESS_URL = '/test/xkx/wifi_type/*'

DEVICE_UPDATER_PROBE_FIELDS_CHANGE = {"start_time": "time_on"}
DEVICE_UPDATER_WIFI_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_AUDIT_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_IM_FIELDS_CHANGE = {"collect_time": "time_on"}

REDIS_HOST = '192.168.1.99'
REDIS_DB = 2
REDIS_PORT = '6379'
REDIS_PASSWORD = 'ggsddu@police'

HDFS_HOSTS = [
    '192.168.7.150:9870',
    '192.168.7.151:9870',
    '192.168.7.152:9870',
]

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


class DBOperatorCheckInTimeSync(DBOperator):
    def update_and_get_attr_df(self, search_mac_list):
        """
        更新attr表并返回DataFrame
        """
        attr_data_column = ['attr_id', 'mobile_mac']
        attr_mac_list = list(set(search_mac_list))
        current_ts = int(time.time() * 1000)
        conn = self.db_conn_pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor()

        if len(attr_mac_list) > 1:
            mac_tuple = tuple(map(lambda x: f"'{x}'" if x.isdigit() else x, attr_mac_list))
        else:
            mac_tuple = f"('{attr_mac_list[0]}')"

        sql = f"SELECT id,attr_value FROM zhaoqing_duanzhou_db.attr WHERE attr_type_id = 5 AND attr_value in {mac_tuple} and sync_in_time = false"
        cursor.execute(sql)
        exist_attr_query_result = cursor.fetchall()
        exist_attr_df = pd.DataFrame(exist_attr_query_result, columns=attr_data_column)
        exist_attr_value_list = exist_attr_df['mobile_mac'].tolist()

        not_exist_attr_value_list = list(
            (collections.Counter(attr_mac_list) - collections.Counter(exist_attr_value_list)).elements()
        )

        if not_exist_attr_value_list:
            insert_values_str = ""
            for attr_mac in not_exist_attr_value_list:
                insert_values_str += f"(5,'{attr_mac}',{current_ts},{current_ts},false),"
            insert_values_str = insert_values_str[:-1]

            cursor.execute(
                f"insert into {self.schema}.{self.attr_table} " +
                f"(attr_type_id,attr_value,create_time,update_time,sync_in_time)" +
                f"values {insert_values_str} " +
                f"on conflict(attr_type_id, attr_value) " +
                f"do update set update_time={current_ts} " +
                f"RETURNING id,attr_value"
            )
            conn.commit()
            query_record = cursor.fetchall()
            new_attr_df = pd.DataFrame(query_record, columns=attr_data_column)
            cursor.close()
            self.db_conn_pool.putconn(conn)
            attr_df = pd.concat([exist_attr_df, new_attr_df])
            return attr_df
        else:
            cursor.close()
            self.db_conn_pool.putconn(conn)
            return exist_attr_df

    def update_and_get_track_df(self, track_df):
        track_data = track_df.to_dict(orient="records")
        track_data_column = ['track_id', 'datasource_id', 'netbar_wacode']
        insert_track_df = pd.DataFrame(columns=track_data_column)
        insert_data_str = ""
        tmp_count = 0
        global track_write_time
        for data in track_data:
            insert_data_str += f"({data['probe_time']},'{data['netbar_wacode']}',{data['create_time']},{data['datasource_id']},'{data['datasource_table_name']}',{data['probe_device_id']}),"
            tmp_count += 1
            if tmp_count >= 100000 and insert_data_str:
                conn = self.db_conn_pool.getconn()
                cursor = conn.cursor()
                st_ts = time.time()
                insert_data_str = insert_data_str[:-1]
                cursor.execute(
                    f"SELECT setval('zhaoqing_duanzhou_db.track_id_seq', (SELECT max(id) FROM {self.schema}.{self.track_table}));"
                )
                cursor.execute(
                    f"insert into {self.schema}.{self.track_table} " +
                    f"(probe_time,netbar_wacode,create_time,datasource_id,datasource_table_name,probe_device_id) " +
                    f"values {insert_data_str} " +
                    f"RETURNING id,datasource_id,netbar_wacode"
                )
                conn.commit()
                insert_query_result = cursor.fetchall()
                part_insert_track_df = pd.DataFrame(insert_query_result, columns=track_data_column)
                insert_track_df = pd.concat([insert_track_df, part_insert_track_df])

                end_ts = time.time()
                spend_time = int((end_ts - st_ts))
                track_write_time.append(spend_time)
                insert_data_str = ""
                tmp_count = 0
                cursor.close()
                self.db_conn_pool.putconn(conn)

        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        insert_data_str = insert_data_str[:-1]
        cursor.execute(
            f"SELECT setval('zhaoqing_duanzhou_db.track_id_seq', (SELECT max(id) FROM {self.schema}.{self.track_table}));"
        )
        cursor.execute(
            f"insert into {self.schema}.{self.track_table} " +
            f"(probe_time,netbar_wacode,create_time,datasource_id,datasource_table_name,probe_device_id) " +
            f"values {insert_data_str} " +
            f"RETURNING id,datasource_id,netbar_wacode"
        )
        conn.commit()
        insert_query_result = cursor.fetchall()
        part_insert_track_df = pd.DataFrame(insert_query_result, columns=track_data_column)
        insert_track_df = pd.concat([insert_track_df, part_insert_track_df])

        cursor.close()
        self.db_conn_pool.putconn(conn)
        return insert_track_df

    def update_attr_record_table(self, attr_record_df):
        attr_record_data = attr_record_df.to_dict(orient="records")
        insert_data_str = ""
        tmp_count = 0
        for data in attr_record_data:
            insert_data_str += f"({data['track_id']},{data['attr_id']},{data['create_time']}),"
            if tmp_count >= 100000 and insert_data_str:
                conn = self.db_conn_pool.getconn()
                cursor = conn.cursor()
                insert_data_str = insert_data_str[:-1]
                cursor.execute(
                    f"SELECT setval('zhaoqing_duanzhou_db.person_attr_record_id_seq', (SELECT max(id) FROM {self.schema}.{self.attr_record_table}));"
                )
                cursor.execute(
                    f"insert into {self.schema}.{self.attr_record_table} " +
                    f"(track_id,attr_id,create_time) " +
                    f"values {insert_data_str} " +
                    f"RETURNING id"
                )
                conn.commit()
                insert_data_str = ""
                tmp_count = 0
                conn.commit()
                cursor.close()
                self.db_conn_pool.putconn(conn)

        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        insert_data_str = insert_data_str[:-1]
        cursor.execute(
            f"SELECT setval('zhaoqing_duanzhou_db.person_attr_record_id_seq', (SELECT max(id) FROM {self.schema}.{self.attr_record_table}));"
        )
        cursor.execute(
            f"insert into {self.schema}.{self.attr_record_table} " +
            f"(track_id,attr_id,create_time) " +
            f"values {insert_data_str}"
        )
        conn.commit()
        cursor.close()
        self.db_conn_pool.putconn(conn)

    def update_case(self, case_id):
        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE zhaoqing_duanzhou_db.case SET status = 0 WHERE id={case_id}"
        )
        conn.commit()
        cursor.close()
        self.db_conn_pool.putconn(conn)

    def delete_case(self, case_id):
        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE zhaoqing_duanzhou_db.case SET status = -1 WHERE id={case_id}"
        )
        conn.commit()
        cursor.close()
        self.db_conn_pool.putconn(conn)


class DBOperatorTrackSync(DBOperator):
    def __init__(self):
        super().__init__()
        self.mac_list = None

    def update_and_get_attr_df(self, search_mac_list):
        """
        更新attr表并返回DataFrame
        """
        attr_data_column = ['attr_id', 'mobile_mac']
        attr_mac_list = list(set(search_mac_list))
        current_ts = int(time.time() * 1000)
        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()

        if len(attr_mac_list) > 1:
            mac_tuple = tuple(map(lambda x: f"'{x}'" if x.isdigit() else x, attr_mac_list))
        else:
            mac_tuple = f"('{attr_mac_list[0]}')"
        sql = f"SELECT attr_value FROM zhaoqing_duanzhou_db.attr WHERE attr_type_id = 5 AND attr_value in {mac_tuple} AND sync_in_time = true"
        cursor.execute(sql)
        query_result = cursor.fetchall()
        if query_result:
            exist_attr_value_list = [result[0] for result in query_result]
        else:
            exist_attr_value_list = list()
        not_exist_attr_value_list = list(
            (collections.Counter(self.mac_list) - collections.Counter(exist_attr_value_list)).elements()
        )
        if not_exist_attr_value_list:
            if len(not_exist_attr_value_list) > 1:
                update_mac_tuple = tuple(map(lambda x: f"'{x}'" if x.isdigit() else x, not_exist_attr_value_list))
            else:
                update_mac_tuple = f"('{not_exist_attr_value_list[0]}')"
            cursor.execute(
                f"UPDATE zhaoqing_duanzhou_db.attr SET sync_in_time = true WHERE attr_type_id = 5 and attr_value in {update_mac_tuple} AND sync_in_time = false RETURNING id,attr_value"
            )
            conn.commit()
            update_query_result = cursor.fetchall()
            update_attr_df = pd.DataFrame(update_query_result, columns=attr_data_column)
            if not update_attr_df.empty:
                update_mac_list = update_attr_df['mobile_mac'].tolist()
            else:
                update_mac_list = list()
            insert_attr_value_list = list(
                (collections.Counter(not_exist_attr_value_list) - collections.Counter(update_mac_list)).elements()
            )
            if insert_attr_value_list:
                insert_values_str = ""
                for attr_mac in insert_attr_value_list:
                    insert_values_str += f"(5,'{attr_mac}',{current_ts},{current_ts}),"
                insert_values_str = insert_values_str[:-1]

                cursor.execute(
                    f"insert into {self.schema}.{self.attr_table} " +
                    f"(attr_type_id,attr_value,create_time,update_time) " +
                    f"values {insert_values_str} " +
                    f"on conflict(attr_type_id, attr_value) " +
                    f"do update set update_time={current_ts} " +
                    f"RETURNING id,attr_value"
                )
                conn.commit()
                query_record = cursor.fetchall()
                attr_df = pd.DataFrame(query_record, columns=attr_data_column)
                cursor.close()
                self.db_conn_pool.putconn(conn)
                return pd.concat([attr_df, update_attr_df])
            else:
                cursor.close()
                self.db_conn_pool.putconn(conn)
                return update_attr_df
        else:
            cursor.close()
            self.db_conn_pool.putconn(conn)
            return pd.DataFrame()

    def update_and_get_track_df(self, track_df):
        tar_df = None
        track_data = track_df.to_dict(orient="records")
        while True:
            if len(track_data) > 10000:
                part_track_data = track_data[:10000]
                track_data = track_data[10000:]
                attr_part_df = self._update_track(part_track_data)
                if tar_df is None:
                    tar_df = attr_part_df
                else:
                    tar_df = pd.concat([tar_df, attr_part_df])
            else:
                attr_part_df = self._update_track(track_data)
                if tar_df is None:
                    tar_df = attr_part_df
                else:
                    tar_df = pd.concat([tar_df, attr_part_df])
                break
        return tar_df

    def _update_track(self, track_data):
        track_data_column = ['track_id', 'datasource_id']
        insert_data_str = ""
        for data in track_data:
            insert_data_str += f"({data['probe_time']},'{data['netbar_wacode']}',{data['create_time']},{data['datasource_id']},'{data['datasource_table_name']}',{data['probe_device_id']}),"
        insert_data_str = insert_data_str[:-1]
        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT setval('zhaoqing_duanzhou_db.track_id_seq', (SELECT max(id) FROM {self.schema}.{self.track_table}));"
        )
        cursor.execute(
            f"insert into {self.schema}.{self.track_table} " +
            f"(probe_time,netbar_wacode,create_time,datasource_id,datasource_table_name,probe_device_id) " +
            f"values {insert_data_str} " +
            f"RETURNING id,datasource_id"
        )
        conn.commit()

        insert_query_result = cursor.fetchall()
        insert_track_df = pd.DataFrame(insert_query_result, columns=track_data_column)
        cursor.close()
        self.db_conn_pool.putconn(conn)
        return insert_track_df

    def update_attr_record_table(self, attr_record_df):
        attr_record_data = attr_record_df.to_dict(orient="records")
        while True:
            if len(attr_record_data) > 10000:
                part_attr_record_data = attr_record_data[:10000]
                attr_record_data = attr_record_data[10000:]
                self._update_attr_record(part_attr_record_data)
            else:
                self._update_attr_record(attr_record_data)
                break

    def _update_attr_record(self, attr_record_data):
        insert_data_str = ""
        for data in attr_record_data:
            insert_data_str += f"({data['track_id']},{data['attr_id']},{data['create_time']}),"
        insert_data_str = insert_data_str[:-1]
        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT setval('zhaoqing_duanzhou_db.person_attr_record_id_seq', (SELECT max(id) FROM {self.schema}.{self.attr_record_table}));"
        )
        cursor.execute(
            f"insert into {self.schema}.{self.attr_record_table} " +
            f"(track_id,attr_id,create_time) " +
            f"values {insert_data_str}"
        )
        conn.commit()
        cursor.close()
        self.db_conn_pool.putconn(conn)



# 'probe_time', 'probe_mac', 'place_code', 'collect_mac'
probe_fields_change = {'start_time': 'probe_time'}
wifi_fields_change = {'collect_time': 'probe_time',
                      'wifi_mac': 'probe_mac'}
hdfs_host = '192.168.7.150'
hdfs_audit_path = '/audit_data/weihai/audit_type'
hdfs_im_path = '/audit_data/weihai/im_type'

class AnalysisEleFence_bak(object):
    def __init__(self, spark):
        self.spark = spark
        self.precision_span = 60
        self.timestack_span = 600
        self.udf = ETL_Udf()
        self.HDFS_CLIENT = pyhdfs.HdfsClient(hosts=hdfs_host+':9870', user_name='hdfs')
        self.HDFS_AUDIT = hdfs_audit_path
        self.HDFS_IM = hdfs_im_path

    def audit_union(self, df, path):
        if self.HDFS_CLIENT.exists(path):
            file_path_list = self.HDFS_CLIENT.listdir(path)
            if file_path_list:
                df_old = self.spark.read.parquet(path + '/' + file_path_list[0])
                df = df.union(df_old)
                df = df.dropDuplicates()
                df.write.parquet(path + '/' + str(time.time()))
                self.HDFS_CLIENT.delete(path + '/' + file_path_list[0])
                return 0
            df.write.parquet(path + '/' + str(time.time()))

    def etl(self, source_path, target_path, filename, tablename):
        """对所有的probe_mac进行清洗，并入库"""
        df = self.spark.read.csv(source_path + filename, header=True)
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
        elif tablename == 'wifi_type':
            for i in range(len(wifi_fields_change)):
                df = df.withColumnRenamed(wifi_key_c[i], wifi_val_c[i])
        else:
            print("tablename is not probe_type or wifi_type!")
            return 0
        df_ele_fence = df.select('probe_time', 'probe_mac', 'place_code', 'collect_mac')
        # 将probe_time精度调整为一分钟
        # func_rounded_up = udf(lambda x: udf_rounded_up(x, self.precision_span), LongType())
        func_rounded_up = udf(lambda x: x and self.udf.udf_rounded_up(x, 60) or 0, LongType())
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
        func_2_list_string = udf(self.udf.udf_2_list_string, StringType())
        df_ele_fence = df_ele_fence.withColumn("probe_time", func_2_list_string("probe_time"))

        # 将[probe_time, max_count]按probe_mac和place_code分组，并合成list
        df_ele_fence = df_ele_fence.groupBy(["probe_mac", "place_code"]).agg(
            functions.collect_list('probe_time').alias('time_list'))  # .drop("max(count)")
        # 删除没用的数据并按一定规则生成轨迹（开始时间+结束时间）
        # func_etl = udf(lambda x: udf_time_etl(x, self.timestack_span), StringType())
        func_etl = udf(lambda x: self.udf.udf_time_etl(x, 600), StringType())
        df_ele_fence = df_ele_fence.withColumn("time_list", func_etl("time_list"))
        # explode成多个轨迹，并将轨迹拆分成time_start、time_end、count
        df_ele_fence = df_ele_fence.withColumn("time_list", explode(split("time_list", "\\]', '\\[")))
        df_ele_fence = df_ele_fence.withColumn("time_start", split("time_list", ",")[0]).withColumn(
            "time_end", split("time_list", ",")[1]).withColumn(
            "count", split("time_list", ",")[2]).drop("time_list")
        df_ele_fence = df_ele_fence.withColumn("time_start", df_ele_fence.time_start.astype("long"))
        func_text2long = udf(self.udf.udf_text2long, LongType())
        df_ele_fence = df_ele_fence.withColumn("time_end", func_text2long("time_end"))
        df_ele_fence = df_ele_fence.withColumn("count", func_text2long("count"))
        df_ele_fence = df_ele_fence.withColumn("probe_mac", df_ele_fence["probe_mac"].astype("string"))
        df_ele_fence = df_ele_fence[df_ele_fence["probe_mac"] != '0']
        # if tablename == 'co_ap':
        #     df_co_ap_ssid = df.select('probe_mac', 'ssid').drop_duplicates()
        #     df_ele_fence = df_ele_fence.join(df_co_ap_ssid, ["probe_mac"], "left")
        func_mac_o2h = udf(self.udf.udf_mac_o2h, StringType())
        df_ele_fence = df_ele_fence.withColumn("probe_mac", func_mac_o2h("probe_mac"))
        df_ele_fence = df_ele_fence.withColumnRenamed('probe_mac', 'mac').withColumnRenamed('time_start', 'time_on').withColumnRenamed('time_end', 'time_off')
        df_ele_fence = df_ele_fence.repartition(1)
        df_ele_fence.write.parquet(target_path + filename, mode='overwrite')


class AnalysisEleFence(object):
    def __init__(self, spark):
        self.spark = spark
        self.precision_span = 60
        self.timestack_span = 600
        self.HDFS_CLIENT = pyhdfs.HdfsClient(hosts=hdfs_host+':9870', user_name='hdfs')
        self.HDFS_AUDIT = hdfs_audit_path
        self.HDFS_IM = hdfs_im_path

    def audit_union(self, df, path):
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
            if 100 < int(df.first()['probe_time']) < 1000000000000:
                func_x1000 = udf(ETL_Udf().udf_x1000, LongType())
                df = df.withColumn("probe_time", func_x1000("probe_time"))
                df = df[df['probe_time'] > 100000]
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
        df_ele_fence = df_ele_fence.withColumnRenamed('probe_mac', 'mac').withColumnRenamed('time_start', 'time_on').withColumnRenamed('time_end', 'time_off')
        time_list = AnalysisEleFence(spark).df_time_block_list(df_ele_fence)
        day_list = [time.strftime("%Y%m%d", time.localtime(int(i/1000))) for i in time_list]
        if not self.HDFS_CLIENT.exists(target_path):
            self.HDFS_CLIENT.mkdirs(target_path)
        hdfs_target_path_list = self.HDFS_CLIENT.listdir(target_path)
        for i in range(len(day_list) - 1):
            df_new = df_ele_fence[df_ele_fence['time_on'] > time_list[i]][df_ele_fence['time_off'] < time_list[i + 1]]
            if day_list[i] in hdfs_target_path_list:
                df = spark.read.parquet(target_path + day_list[i])
                df = df.union(df_new)
                df.write.parquet(target_path + day_list[i] + '_bak', mode='overwrite')
                df = spark.read.parquet(target_path + day_list[i] + '_bak')
                df.write.parquet(target_path + day_list[i], mode='overwrite')
                self.HDFS_CLIENT.delete(target_path + day_list[i] + '_bak', recursive=True)
                continue
            df_new.write.parquet(target_path + day_list[i], mode='overwrite')

    def df_time_block_list(self, df):
        df = df.select('time_on').sort(df.time_on)
        time_on = df.first()[0]
        time_on = int(time_on / 86400000) * 86400000 - 28800000
        df = df.sort(df.time_on.desc())
        time_off = df.first()[0]
        time_off = (int(time_off / 86400000) + 1) * 86400000 - 28800000
        day_count = int((time_off - time_on) / 86400000)
        time_list = []
        for i in range(day_count + 1):
            time_list.append(time_on + i * 86400000)
        return time_list

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    AnalysisEleFence(spark).etl(
            '/test/xkx/probe_type/20201207',
            '/test/cjh/par/probe_type20201207',
            '20201207000104542_371002_755652234_001.parquet',
            'probe_type'
        )

        # 'hdfs://192.168.7.150:8020/test/xkx/demo/probe_type/20201127',
        # 'hdfs://192.168.7.150:8020/test/cjh/par/probe_type20201202',
        # '20201127110502594_430300_755652234_001.parquet',
        # 'probe_type'
    spark.stop()

