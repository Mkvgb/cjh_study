# -*- encoding: utf-8 -*-
"""
@File       :   deviceupdater20201202.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/12/2 15:38
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import uuid
import pyhdfs
import sys
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import pandas as pd
from psycopg2.extras import RealDictCursor
DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '5435'
DST_DB_USER = "postgres"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "zhaoqing_duanzhou_db"
DST_DB_ATTR_TABLE = "attr"
DST_DB_ATTR_RECORD_TABLE = "attr_record"
DST_DB_TRACK_TABLE = "track"

# 字段rename
SPARK_SEARCH_FIELDS_CHANGE = {"time_start": "time_on",
                              "time_end": "time_off",
                              "mobile_mac": "apMac",
                              "netbar_wacode": "placeCode"
                              }
DEVICE_UPDATER_PROBE_FIELDS_CHANGE = {"start_time": "time_on"}
DEVICE_UPDATER_WIFI_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_AUDIT_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_IM_FIELDS_CHANGE = {"collect_time": "time_on"}

# spark初始化配置
SPARK_EXECUTOR_MEMORY_CONF = "spark.executor.memory"    # "3g"
SPARK_DRIVER_MEMORY_CONF = "spark.driver.memory"        # "3g"
SPARK_NUM_EXECUTORS_CONF = "spark.num.executors"        # "3"
SPARK_EXECUTOR_CORE_CONF = "spark.executor.core"        # "3"
SPARK_SCHEDULER_MODE_CONF = "spark.scheduler.mode"      # "FIFO"

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

FILE_PATH_LIST = [
        "/test/xkx/audit_type",
        "/test/xkx/probe_type",
    ]

class UpdateTimeEleFence(object):
    def __init__(self):
        conf = SparkConf().setAppName('deviceupdate')
        conf.set("spark.executor.memory", "3g")
        conf.set("spark.driver.memory", "3g")
        conf.set("spark.executor.core", "3")
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.PGSQL_URL = f"jdbc:postgresql://{DST_DB_HOST}:{DST_DB_PORT}/{DST_DB_DATABASE}"
        self.PGSQL_DEVICE_TABLE = DST_DB_SCHEMA+".gd_device"
        self.PGSQL_PLACE_TABLE = DST_DB_SCHEMA+".gd_place"
        self.PGSQL_PROPERTIES = {'user': DST_DB_USER, 'password': DST_DB_PASSWORD}
        self.HDFS_CLIENT = pyhdfs.HdfsClient(hosts=HDFS_HOST+':9870', user_name='hdfs')

    def updatetimer_func(self, project_name):
        file_path_list = [('/' + project_name + i).replace("//", "/") for i in FILE_PATH_LIST]
        for i in range(len(file_path_list)):
            if self.HDFS_CLIENT.exists(file_path_list[i]):
                file_path_list_tmp = self.HDFS_CLIENT.listdir(file_path_list[i])
                if len(file_path_list_tmp) == 0:
                    print(file_path_list[i] + " is empty!")
                    continue
                file_path = file_path_list[i] + '/' + str(sorted([int(j) for j in file_path_list_tmp])[len(file_path_list_tmp) - 1])
                UpdateTimeEleFence().device_place_info_2hdfs_real_time(file_path)
        self.spark.stop()

    def device_place_info_2hdfs_real_time(self, file_path):
        """更新场所与设备的update_time，并将更新的数据放到hdfs中，但不会更新设备探测次数，需要调用count_dev_probe_2hdfs进行更新"""
        update_job = UpdateTimeEleFence()
        # df_pla_old[place_code, longitude, latitude, update_time],每天的更新以及场所设备的更新需要在这一天实时更新之后,这样才不会动场所设备的内容
        # df_dev_old[device_mac,update_time,place_code,count]

        df_pla_old = self.spark.read.jdbc(url=self.PGSQL_URL, table=self.PGSQL_PLACE_TABLE, properties=self.PGSQL_PROPERTIES)
        df_dev_old = self.spark.read.jdbc(url=self.PGSQL_URL, table=self.PGSQL_DEVICE_TABLE, properties=self.PGSQL_PROPERTIES)


        df_ef = self.spark.read.parquet(file_path)

        df_ef = update_job.prepare_ele_fence(df_ef, file_path)
        if type(df_ef) == int:
            print("table_name must be probe_type, wifi_type, audit_type, im_type!")
        df_dev_new = update_job.update_time_dev(df_ef, df_dev_old)
        update_job.update_time_pla_new(df_dev_new, df_dev_old,  df_pla_old)

    def prepare_ele_fence(self, df_ef, file_path):
        """实时的数据,并进行数据预处理"""
        # device_mac, place_code, time_on
        # 字段修改
        if 'probe_type' in file_path:
            fields_change = DEVICE_UPDATER_PROBE_FIELDS_CHANGE
        elif 'wifi_type' in file_path:
            fields_change = DEVICE_UPDATER_WIFI_FIELDS_CHANGE
        elif 'audit_type' in file_path:
            fields_change = DEVICE_UPDATER_AUDIT_FIELDS_CHANGE
        elif 'im_type' in file_path:
            fields_change = DEVICE_UPDATER_IM_FIELDS_CHANGE
        else:
            return 1
        keys_c = list(fields_change.keys())
        values_c = list(fields_change.values())
        for i in range(len(keys_c)):
            df_ef = df_ef.withColumnRenamed(keys_c[i], values_c[i])
        df_ef = df_ef.select(["place_code", "collect_mac", "time_on"]).withColumnRenamed("collect_mac", "device_mac")
        if int(df_ef.first()['time_on']) < 1000000000000:
            func_x1000 = udf(UdfUtils().udf_x1000, LongType())
            df_ef = df_ef.withColumn("update_time", func_x1000("time_on")).drop("time_on")
        else:df_ef = df_ef.withColumnRenamed('time_on', 'update_time')
        func_null_to_zero = udf(UdfUtils().udf_null_to_zero, StringType())
        df_ef = df_ef.withColumn("device_mac", func_null_to_zero("device_mac")).withColumn("place_code", func_null_to_zero("place_code"))
        df_ef = df_ef[df_ef["device_mac"] != "0"][df_ef["place_code"] != "0"]
        # func_mac_o2h = udf(UdfUtils().udf_mac_o2h, StringType())
        # df_ef = df_ef.withColumn("device_mac", func_mac_o2h("device_mac"))
        # [place_code, device_mac, update_time]
        return df_ef

    def update_time_dev(self, df_ef, df_dev_old):
        """设备表updatetime更新，具体输出dataframe格式为[id, updatetime]"""
        # df_dev_old [device_mac,update_time,place_code,count]
        # df_ef [place_code, device_mac, update_time]
        # _df_dev_new [device_mac, update_time_new]
        df_dev_new = df_ef.select(["device_mac", "update_time"]).groupBy("device_mac").max("update_time").withColumnRenamed("max(update_time)", "update_time_new")
        # _df_dev_old [device_mac,update_time,place_code,count]
        df_dev_old_tmp = df_dev_old.withColumn("update_time", df_dev_old.update_time.astype("long"))
        df_dev_old_tmp = df_dev_new.select("device_mac").intersect(df_dev_old_tmp.select("device_mac")).join(df_dev_old_tmp, ["device_mac"], "left")

        # df_dev_new [device_mac,place_code,count,update_time,update_time_new]
        df_dev_new = df_dev_old_tmp.join(df_dev_new, ["device_mac"], "outer")
        func_select_field = udf(lambda x, y: UdfUtils().udf_select_bigger_filed(x, y), LongType())
        df_dev_new = df_dev_new.withColumn("update_time", func_select_field("update_time", "update_time_new")).drop("update_time_new")
        # df_dev_old = self.spark.read.jdbc(url=self.PGSQL_URL, table="zhaoqing_duanzhou_db.gd_device_copy1", properties=self.PGSQL_PROPERTIES)
        df_dev_old = df_dev_old.select(["device_mac", "id"])
        df_dev_new = df_dev_new.select(["device_mac", "update_time"]).join(df_dev_old, ["device_mac"], "left").drop("device_mac")
        # [id, update_time]
        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_dev_new = df_dev_new.withColumn("update_time", func_null_to_0("update_time"))
        df_dev_new = df_dev_new[df_dev_new["update_time"] > 0]
        df = df_dev_new.toPandas()
        # print(df)
        UpdateTimeEleFence().update_table(df, 'gd_device')

        return df_dev_new

    def update_time_pla_new(self, df_dev_new, df_dev_old,  df_pla_old):
        # [id, place_code, update_time]
        df_pla_old = df_pla_old.select("id", "place_code", "update_time")
        # [id, place_code]
        df_dev_old = df_dev_old.select("id", "place_code")
        # [id, update_time, place_code]
        df_dev_old.show()
        df_dev_new.show()
        df_dev_new = df_dev_new.join(df_dev_old, ["id"], "left")
        # [id, place_code, update_time]
        df_pla_new = df_dev_new.withColumnRenamed("update_time", "update_time_new").drop("id").join(df_pla_old, ["place_code"], "left")

        func_select_field = udf(lambda x, y: UdfUtils().udf_select_bigger_filed(x, y), LongType())
        df_pla_new = df_pla_new.withColumn("update_time", func_select_field("update_time", "update_time_new")).drop("update_time_new")

        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_pla_new = df_pla_new.withColumn("id", func_null_to_0("id"))
        df_pla_new = df_pla_new[df_pla_new["id"] > 0]
        df = df_pla_new.drop("place_code").toPandas()
        # print(df)
        UpdateTimeEleFence().update_table(df, 'gd_place')

    def update_table(self, df, table_name):
        if not df.empty:
            t_data = df.to_dict(orient="records")
            print(t_data)
            query = create_batch_update_sql(DST_DB_SCHEMA, table_name, t_data, 'update_time', "int")
            print(query)
            db_manage = SQLManageFactory().get_manage()
            db_manage.init_conn()
            db_manage.init_cursor(is_server_side_cursor=True)
            db_manage.cursor.execute(query)
            db_manage.conn.commit()
            db_manage.close_conn()

class UdfUtils(object):
    def udf_add(self, data, add):
        return data + add

    def udf_x1000(self, data):
        """某字段所有数字类型数据乘以1000"""
        return int(data) * 1000

    def udf_delete_string_elemt(self, data, elemt):
        """删除指定列中字符串中的某个字符"""
        return data.replace(elemt, "")

    def udf_mac_o2h(self, data):
        s = str(hex(eval(str(data))))[2:].upper().rjust(12, '0')
        lt_s = list(s)
        lt_s.insert(10, '-')
        lt_s.insert(8, '-')
        lt_s.insert(6, '-')
        lt_s.insert(4, '-')
        lt_s.insert(2, '-')
        s = ''.join(lt_s)
        return s

    def udf_null_to_zero(self, data):
        """输入字符串,为空,返回'0'"""
        if not data:
            return "0"
        else:
            return data

    def udf_select_filed(self, data1, data2):
        """一般为判断两个列元素的大小，并按一定规则留下某一字段的数据"""
        if not data2:
            return data1
        elif not data1:
            return data2
        elif data1 != data2:
            return data2
        elif data1 == data2:
            return data1

    def udf_select_bigger_filed(self, data1, data2):
        """一般为判断两个列元素的大小，并按一定规则留下某一字段的数据"""
        if not data1:
            return data2
        elif not data2:
            return data1
        if data1 >= data2:
            return data1
        elif data1 < data2:
            return data2

    def udf_null_to_0(self, data):
        if not data:
            data = 0
        return data


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
            case_str += f" when {str(record['id'])} then {record[field]}"
        elif field_type == "string":
            case_str += f" when {str(record['id'])} then '{record[field]}'"
    data_range = ','.join(id_list)
    query = f"update {scheme}.{table} set {field} = case id {case_str} end where id in({data_range});"
    return query


def append_pgsql(df, table, DST_DB_SCHEMA):
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
    pd_table = pd.io.sql.SQLTable(table, pd_sql_engine, frame=df, index=False, if_exists="append", DST_DB_SCHEMA=DST_DB_SCHEMA)
    pd_table.create()
    sio.seek(0)
    with engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = f"COPY {DST_DB_SCHEMA}.{table} FROM STDIN HEADER DELIMITER '|' CSV"
            cursor.copy_expert(copy_cmd, sio)
        connection.connection.commit()

class PgSQLManage(object):
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def init_conn(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            cursor_factory=RealDictCursor,
            keepalives=2,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

    def create_engine(self):
        from sqlalchemy import create_engine
        engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
            max_overflow=0,  # 超过连接池大小外最多创建的连接
            pool_size=2,  # 连接池大小
            pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
            pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
        )
        return engine

    def get_conn_string(self):
        return f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'

    def init_cursor(self, is_server_side_cursor=False):
        if is_server_side_cursor:
            self.cursor = self.conn.cursor(name=str(uuid.uuid1()))
        else:
            self.cursor = self.conn.cursor()

    def close_conn(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

class SQLManageFactory(object):
    db_manage_map = {
        "pgsql": PgSQLManage
    }

    def __init__(self, host=None, port=None, user=None, password=None, database=None, db_type='pgsql'):
        self.sql_manage = self.db_manage_map[db_type](
            host=host and host or DST_DB_HOST,
            port=port and port or DST_DB_PORT,
            user=user and user or DST_DB_USER,
            password=password and password or DST_DB_PASSWORD,
            database=database and database or DST_DB_DATABASE
        )

    def get_manage(self):
        return self.sql_manage

if __name__ == '__main__':
    UpdateTimeEleFence().updatetimer_func('')
