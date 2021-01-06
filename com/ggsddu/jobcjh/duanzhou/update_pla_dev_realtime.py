# -*- encoding: utf-8 -*-
"""
@File       :   st_update_act_pla_dev.py
@Contact    :   ggsddu.com
@Modify Time:   2020/8/27 16:31
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import logging
import uuid
import time
import json
import pyhdfs
import sys
import copy
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions
from rocketmq.client import Producer, Message
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

MSG_TEMPLATE = {
    "task_name": "update_dev", "exec_type": "pgsql",
    "param": {
        "conn": {
            "host": "192.168.1.99",
            "port": 6543,
            "user": "postgres",
            "password": "postgres",
            "database": "police_analysis_db",
            "schema": "zhaoqing_duanzhou_db",
            "table": None
        },
        "data": None
    }}


class UpdateTimeEleFence(object):
    def __init__(self, spark):
        self.spark = spark
        self.PGSQL_URL1 = "jdbc:postgresql://192.168.7.160:5432/postgres"
        self.PGSQL_URL_analysis = "jdbc:postgresql://192.168.7.160:5432/police_analysis_db"
        self.PGSQL_PROPERTIES = {'user': 'postgres', 'password': 'postgres'}
        self.HDFS_ST_PLACE = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place'
        self.HDFS_ST_DEVICE = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device'
        self.HDFS_ST_PLACE_TMP = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place_tmp'
        self.HDFS_ST_DEVICE_TMP = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device_tmp'
        self.HDFS_ST_PLACE_INFO = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place_info'
        self.HDFS_ST_DEVICE_INFO = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device_info'

    def device_place_info_2hdfs_real_time(self, date, table_name):

        """更新场所与设备的update_time，并将更新的数据放到hdfs中，但不会更新设备探测次数，需要调用count_dev_probe_2hdfs进行更新"""
        update_job = UpdateTimeEleFence(self.spark)
        # df_pla_old[netbar_wacode, longitude, latitude, update_time],每天的更新以及场所设备的更新需要在这一天实时更新之后,这样才不会动场所设备的内容
        # df_dev_old[device_mac,update_time,netbar_wacode,count]
        df_pla_old = self.spark.read.csv(self.HDFS_ST_PLACE, header=True)
        df_pla_old = df_pla_old.select(["netbar_wacode", "longitude", "latitude", "update_time"])
        df_dev_old = self.spark.read.csv(self.HDFS_ST_DEVICE, header=True)
        # df_ef = self.spark.read.parquet(date)
        sch_ap = StructType([
            StructField("id", LongType()), StructField("ap_mac", LongType()), StructField("str_ap_mac", StringType()), StructField("ssid", StringType()), StructField("signal", StringType()), StructField("channel", LongType()),
            StructField("encryption_type", LongType()), StructField("company", StringType()), StructField("time_on", LongType()), StructField("time_off", LongType()), StructField("time_update", LongType()),
            StructField("local_mac", LongType()),
            StructField("place_code", LongType())
        ])

        # df_ef = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/co_ap_list_1588528801.csv', schema=sch_ap)
        # df_ef = spark.read.parquet('hdfs://192.168.7.150:8020/data_sync/gd_ele_fence/202007210800/202007210800.0.snappy.parquet')
        df_pla = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_place", properties=self.PGSQL_PROPERTIES)
        df_ef = update_job.prepare_ele_fence(df_ef)
        # _df_pla_old[netbar_wacode, update_time]
        _df_pla_old = update_job.prepare_pla(df_pla_old)
        # df_dev_old[device_mac,update_time,netbar_wacode,count]
        df_dev_old = update_job.prepare_dev(df_dev_old)
        # update_job.update_time_pla(df_ef, _df_pla_old, df_pla_old)
        update_job.update_dev(df_ef, table_name)
        df_dev_new = update_job.update_time_dev(df_ef, df_dev_old)
        update_job.update_time_pla_new(df_dev_new)

    def prepare_ele_fence(self, df_ef):
        """实时的数据,并进行数据预处理"""
        df_ef = df_ef.withColumnRenamed("ap_mac", "mobile_mac").withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("local_mac", "collection_equipment_mac").withColumnRenamed("router_mac", "collection_equipment_mac")
        df_ef = df_ef.select(["netbar_wacode", "collection_equipment_mac", "time_on"]).withColumnRenamed("collection_equipment_mac", "device_mac")
        func_x1000 = udf(UdfUtils().udf_x1000, LongType())
        df_ef = df_ef.withColumn("update_time", func_x1000("time_on")).drop("time_on")
        func_null_to_zero = udf(UdfUtils().udf_null_to_zero, StringType())
        df_ef = df_ef.withColumn("device_mac", func_null_to_zero("device_mac")).withColumn("netbar_wacode", func_null_to_zero("netbar_wacode"))
        df_ef = df_ef[df_ef["device_mac"] != "0"][df_ef["netbar_wacode"] != "0"]
        func_mac_o2h = udf(UdfUtils().udf_mac_o2h, StringType())
        df_ef = df_ef.withColumn("device_mac", func_mac_o2h("device_mac"))
        # [netbar_wacode, device_mac, update_time]
        return df_ef

    def prepare_pla(self, df_pla_old):
        """旧place表数据准备，进行数据预处理"""
        # df_pla_old = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place_source', header=True).select(["netbar_wacode", "longitude", "latitude", "update_time"])
        df_pla_old.write.csv(self.HDFS_ST_PLACE_TMP, header=True, mode="overwrite")
        df_pla_old = self.spark.read.csv(self.HDFS_ST_PLACE_TMP, header=True)
        # df_pla_old = df_pla_old.select(["netbar_wacode", "longitude", "latitude", "update_time"])
        _df_pla_old = df_pla_old.select(["netbar_wacode", "update_time"])
        return _df_pla_old

    def prepare_dev(self, df_dev_old):
        """旧device表数据准备，进行数据预处理"""
        # [netbar_wacode, device_mac, update_time]
        # df_dev_old = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device_source', header=True).select(["netbar_wacode", "device_mac", "update_time"])
        df_dev_old.write.csv(self.HDFS_ST_DEVICE_TMP, header=True, mode="overwrite")
        df_dev_old = self.spark.read.csv(self.HDFS_ST_DEVICE_TMP, header=True)
        return df_dev_old

    def update_time_pla(self, df_ef, _df_pla_old, df_pla_old):
        """场所表updatetime更新，即更新新场所，又更新时间，具体输出dataframe格式为[id, updatetime]"""
        # df_pla_old[netbar_wacode, longitude, latitude, update_time]
        # _df_pla_old[netbar_wacode, update_time]
        # df_ef [netbar_wacode, device_mac, update_time]
        # _df_pla_new[netbar_wacode, update_time_new]
        _df_pla_new = df_ef.select(["netbar_wacode", "update_time"]).groupBy("netbar_wacode").max("update_time").withColumnRenamed("max(update_time)", "update_time_new")
        _df_pla_old = _df_pla_old.withColumn("update_time", _df_pla_old.update_time.astype("long"))
        # df_pla_new[netbar_wacode, update_time, update_time_new]
        df_pla_new = _df_pla_old.join(_df_pla_new, ["netbar_wacode"], "outer")
        func_select_field = udf(lambda x, y: UdfUtils().udf_select_filed(x, y), StringType())
        # df_pla_new[netbar_wacode, update_time]
        df_pla_new = df_pla_new.withColumn("update_time", func_select_field("update_time", "update_time_new")).drop("update_time_new")
        # df_pla_new[netbar_wacode, update_time, longitude, latitude]
        df_pla_new = df_pla_new.join(df_pla_old.drop("update_time"), ["netbar_wacode"], "left")
        df_pla_old = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_place", properties=self.PGSQL_PROPERTIES)
        func_delete_space = udf(lambda x: UdfUtils().udf_delete_string_elemt(x, " "), StringType())
        # df_pla_old[netbar_wacode, id]
        df_pla_old = df_pla_old.withColumn("netbar_wacode", func_delete_space("netbar_wacode")).select(["netbar_wacode", "id"])

        df_pla_sub = df_pla_new.select("netbar_wacode").subtract(df_pla_old.select("netbar_wacode"))
        df_pla_sub = df_pla_sub.join(df_pla_new, ["netbar_wacode"], "left")
        df_pla_sub = df_pla_sub.withColumn("update_time", df_pla_sub.update_time.astype("long")).withColumn("create_time", functions.lit(int(time.time() * 1000))).withColumn("company_code", functions.lit("755652234"))
        df_pla_sub_count = df_pla_sub.count()
        if df_pla_sub_count > 0:
            df_pla_sub = df_pla_sub.toPandas()
            df_dev_id = spark.range(df_pla_sub_count).toPandas()
            df_max_id = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_place", properties=self.PGSQL_PROPERTIES).select("id")
            max_id = df_max_id.sort(df_max_id["id"].desc()).first()[0]
            df_dev_id["id"] = df_dev_id["id"].map(lambda x: UdfUtils().udf_add(x, max_id + 1))
            df_dev = pd.concat([df_pla_sub, df_dev_id], axis=1, join='inner')
            df_pla_sub = self.spark.createDataFrame(df_dev)

            df_pla_sub.write.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_place", properties=self.PGSQL_PROPERTIES, mode="append")

        # df_pla_old有的才更新
        # df_pla_new[id, update_time]
        df_pla_new = df_pla_old.join(df_pla_new.select(["netbar_wacode", "update_time"]), ["netbar_wacode"], "left").drop("netbar_wacode")

        df_pla_new = df_pla_new.withColumn("update_time", df_pla_new.update_time.astype("long"))
        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_pla_new = df_pla_new.withColumn("update_time", func_null_to_0("update_time"))
        # 去除掉没有更新的场所，即update_time为空
        df_pla_new = df_pla_new[df_pla_new["update_time"] > 0]
        df = df_pla_new.toPandas()
        if not df.empty:
            t_data = df.to_dict(orient="records")
            query = create_batch_update_sql("zhaoqing_duanzhou_db", "gd_place", t_data, 'update_time', "int")
            with PGSQLOpr("192.168.7.160", 5432, "police_analysis_db", "postgres", "postgres") as opr:
                opr.update_cursor()
                opr.cursor.execute(query)
                opr.conn.commit()

    def update_dev_total(self):
        df_dev = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_device", properties=self.PGSQL_PROPERTIES)
        df_pla = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_place", properties=self.PGSQL_PROPERTIES)
        df_dev = df_dev.select("netbar_wacode", "device_mac").groupBy("netbar_wacode").count().drop("device_mac")
        # [id, device_total]
        df_update = df_pla.select("id", "netbar_wacode").join(df_dev, ["netbar_wacode"], "left").drop("netbar_wacode").withColumnRenamed("count", "device_total")
        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_update = df_update.withColumn("device_total", func_null_to_0("device_total"))
        df = df_update.toPandas()
        if not df.empty:
            t_data = df.to_dict(orient="records")
            query = create_batch_update_sql("zhaoqing_duanzhou_db", "gd_place", t_data, 'device_total', "int")
            with PGSQLOpr("192.168.7.160", 5432, "police_analysis_db", "postgres", "postgres") as opr:
                opr.update_cursor()
                opr.cursor.execute(query)
                opr.conn.commit()

    def update_dev(self, df_ef, table_name):
        """更新设备表的新增设备"""
        _df_dev_new = df_ef.select(["device_mac", "update_time"]).groupBy("device_mac").max("update_time").withColumnRenamed("max(update_time)", "update_time_new")
        # _df_ef_dev_count[netbar_wacode, device_mac, count]
        _df_ef_dev_count = df_ef.groupBy(["netbar_wacode", "device_mac"]).count()
        # _df_ef_dev_true[device_mac, count]
        _df_ef_dev_true = _df_ef_dev_count.groupBy("device_mac").max("count").withColumnRenamed("max(count)", "count")  # .drop("max(count)")
        _df_ef_dev_true = _df_ef_dev_true[_df_ef_dev_true["count"] >= 3]
        # _df_ef_dev_true [netbar_wacode, device_mac]
        _df_ef_dev_true = _df_ef_dev_true.join(_df_ef_dev_count, ["device_mac", "count"], "left").drop("count")
        df_dev_update_source = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_device", properties=self.PGSQL_PROPERTIES)
        # _df_ef_dev_true中为新增的场所设备信息，可能是场所错误，也可能是缺此场所
        _df_ef_dev_true = _df_ef_dev_true.select(concat_ws(',', _df_ef_dev_true.device_mac, _df_ef_dev_true.netbar_wacode).alias('tmp'))
        _df_ef_dev_true = _df_ef_dev_true.withColumn("netbar_wacode", split("tmp", ",")[1])
        _df_ef_dev_true = _df_ef_dev_true.withColumn("device_mac", split("tmp", ",")[0])
        _df_ef_dev_true = _df_ef_dev_true.drop("tmp")
        _df_ef_dev_true = _df_ef_dev_true.subtract(df_dev_update_source.select("netbar_wacode", "device_mac").dropDuplicates(['netbar_wacode', 'device_mac']))
        flag_count = _df_ef_dev_true.count()
        _df_ef_dev_true = _df_ef_dev_true.join(_df_dev_new, ["device_mac"], "left")
        _df_ef_dev_true = _df_ef_dev_true.withColumnRenamed("update_time_new", "update_time")
        df_dev_sub = _df_ef_dev_true.withColumn("update_time", _df_ef_dev_true.update_time.astype("long")).withColumn("create_time", functions.lit(int(time.time() * 1000))).withColumn("type", functions.lit(1))

        df_dev_update_source = df_dev_update_source.select("id", "device_mac", "netbar_wacode")
        # 选出场所信息错误的设备号
        df_dev_update_tmp = df_dev_update_source.select("device_mac").intersect(df_dev_sub.select("device_mac"))

        # 这里需要判断下，如果是安乡的数据就不需要更新错误的场所
        if flag_count > 0 and table_name == 'gd_ele_fence':
            df_dev_update = df_dev_update_tmp.join(df_dev_update_source.select("id", "device_mac"), ["device_mac"], "left")
            # [id, device_mac, netbar_wacode], 用于更新场所号
            df_dev_update = df_dev_update.join(df_dev_sub.select("device_mac", "netbar_wacode"), ["device_mac"], "left")
            # [id, netbar_wacode]
            df_dev_update = df_dev_update.drop("device_mac")
            df = df_dev_update.toPandas()
            if not df.empty:
                t_data = df.to_dict(orient="records")
                query = create_batch_update_sql("zhaoqing_duanzhou_db", "gd_device", t_data, 'netbar_wacode', "string")
                with PGSQLOpr("192.168.7.160", 5432, "police_analysis_db", "postgres", "postgres") as opr:
                    opr.update_cursor()
                    opr.cursor.execute(query)
                    opr.conn.commit()

        df_dev_sub_tmp = df_dev_sub.select("device_mac").subtract(df_dev_update_tmp)
        # 新的场所设备号，append到场所表中
        df_dev_sub = df_dev_sub_tmp.join(df_dev_sub, ["device_mac"], "left")
        df_dev_sub = df_dev_sub.withColumn("collection_equipment_type", functions.lit(3))
        df_dev_sub_count = df_dev_sub.count()
        if df_dev_sub_count > 0:
            df_dev_sub = df_dev_sub.toPandas()
            df_dev_id = spark.range(df_dev_sub_count).toPandas()
            df_max_id = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_device", properties=self.PGSQL_PROPERTIES).select("id")
            max_id = df_max_id.sort(df_max_id["id"].desc()).first()[0]
            df_dev_id["id"] = df_dev_id["id"].map(lambda x: UdfUtils().udf_add(x, max_id + 1))
            df_dev = pd.concat([df_dev_sub, df_dev_id], axis=1, join='inner')
            df_dev_sub = self.spark.createDataFrame(df_dev)

            df_dev_sub.drop("type").write.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_device", properties=self.PGSQL_PROPERTIES, mode="append")

        if flag_count > 0:
            UpdateTimeEleFence(self.spark).update_dev_total()

    def update_time_dev(self, df_ef, df_dev_old):
        """设备表updatetime更新，具体输出dataframe格式为[id, updatetime]"""
        # df_dev_old [device_mac,update_time,netbar_wacode,count]
        # df_ef [netbar_wacode, device_mac, update_time]
        # _df_dev_new [device_mac, update_time_new]
        _df_dev_new = df_ef.select(["device_mac", "update_time"]).groupBy("device_mac").max("update_time").withColumnRenamed("max(update_time)", "update_time_new")
        # _df_dev_old [device_mac,update_time,netbar_wacode,count]
        _df_dev_old = df_dev_old.withColumn("update_time", df_dev_old.update_time.astype("long"))
        _df_dev_old = _df_dev_new.select("device_mac").intersect(_df_dev_old.select("device_mac")).join(_df_dev_old, ["device_mac"], "left")

        # df_dev_new [device_mac,netbar_wacode,count,update_time,update_time_new]
        df_dev_new = _df_dev_old.join(_df_dev_new, ["device_mac"], "outer")
        func_select_field = udf(lambda x, y: UdfUtils().udf_select_bigger_filed(x, y), LongType())
        df_dev_new = df_dev_new.withColumn("update_time", func_select_field("update_time", "update_time_new")).drop("update_time_new")
        df_dev_old = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_device", properties=self.PGSQL_PROPERTIES)
        df_dev_old = df_dev_old.select(["device_mac", "id", "update_time"]).withColumnRenamed("update_time", "update_time_old")

        df_dev_new = df_dev_new.select(["device_mac", "update_time"]).join(df_dev_old, ["device_mac"], "left").drop("device_mac")
        # [id, update_time]

        df_dev_new = df_dev_new.withColumn("update_time", func_select_field("update_time", "update_time_old")).drop("update_time_old")

        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_dev_new = df_dev_new.withColumn("update_time", func_null_to_0("update_time"))
        df_dev_new = df_dev_new[df_dev_new["update_time"] > 0]
        df = df_dev_new.toPandas()
        if not df.empty:
            t_data = df.to_dict(orient="records")
            query = create_batch_update_sql("zhaoqing_duanzhou_db", "gd_device", t_data, 'update_time', "int")
            with PGSQLOpr("192.168.7.160", 5432, "police_analysis_db", "postgres", "postgres") as opr:
                opr.update_cursor()
                opr.cursor.execute(query)
                opr.conn.commit()
        return df_dev_new

    def update_time_pla_new(self, df_dev_new):
        # [id, netbar_wacode, update_time]
        df_pla_old = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_place", properties=self.PGSQL_PROPERTIES).select("id", "netbar_wacode", "update_time")
        # [id, netbar_wacode]
        df_dev_old = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_device", properties=self.PGSQL_PROPERTIES).select("id", "netbar_wacode")
        # [id, update_time, netbar_wacode]
        df_dev_new = df_dev_new.join(df_dev_old, ["id"], "left")
        # [id, netbar_wacode, update_time]
        df_pla_new = df_dev_new.withColumnRenamed("update_time", "update_time_new").drop("id").join(df_pla_old, ["netbar_wacode"], "left")

        func_select_field = udf(lambda x, y: UdfUtils().udf_select_bigger_filed(x, y), LongType())
        df_pla_new = df_pla_new.withColumn("update_time", func_select_field("update_time", "update_time_new")).drop("update_time_new")

        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_pla_new = df_pla_new.withColumn("id", func_null_to_0("id"))
        df_pla_new = df_pla_new[df_pla_new["id"] > 0]
        df = df_pla_new.drop("netbar_wacode").toPandas()
        if not df.empty:
            t_data = df.to_dict(orient="records")
            query = create_batch_update_sql("zhaoqing_duanzhou_db", "gd_place", t_data, 'update_time', "int")
            with PGSQLOpr("192.168.7.160", 5432, "police_analysis_db", "postgres", "postgres") as opr:
                opr.update_cursor()
                opr.cursor.execute(query)
                opr.conn.commit()


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


if __name__ == '__main__':
    conf = SparkConf().setAppName('device')
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.executor.core", "2")
    conf.set("spark.scheduler.mode", "FIFO")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # if len(sys.argv) > 1:
    #     conf_param = json.loads(sys.argv[1])
    #     UpdateTimeEleFence(spark).device_place_info_2hdfs_real_time(conf_param['file_path'], conf_param['table_name'])
    UpdateTimeEleFence(spark).device_place_info_2hdfs_real_time('1', 'co_ele_fence')
    spark.stop()

