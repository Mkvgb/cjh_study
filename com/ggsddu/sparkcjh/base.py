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

from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions
from pyspark.sql.window import Window
from io import StringIO
from sqlalchemy import create_engine
import psycopg2
from pyspark import SparkConf
from pyspark.sql import *
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


if __name__ == '__main__':
    # print("======1:" + str(time.asctime(time.localtime(time.time()))))
    conf = SparkConf().setAppName('cjh')
    # conf.set("sparkcjh.driver.memory", "8g")
    # conf.set("sparkcjh.executor.memory", "8g")
    # conf.set("sparkcjh.driver.memory", "2g")
    # conf.set("sparkcjh.executor.memory", "8g")
    # conf.set("sparkcjh.num.executors", "3")
    # conf.set("sparkcjh.executor.core", "3")
    # conf.set("sparkcjh.default.parallelism", "20")
    # conf.set("sparkcjh.memory.fraction", "0.75")
    # conf.set("sparkcjh.memory.storageFraction", "0.75")
    # conf.set("spark.scheduler.mode", "FIFO")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # CjhTest(spark).cjhtest()
    # spark.range(10).show()

    # def time_c(time):
    #     timeArray = time.localtime(time)
    #     otherStyleTime = time.strftime("%Y--%m--%d", timeArray)
    #     return otherStyleTime
    # lt = ['1588183201.csv']
    # for i in range(len(lt)):
    #     df = df.withColumn("time_on", df["time_on"].astype("long"))
    #     df = spark.read.csv('hdfs://43.58.5.249:8020/co_ele_fence/' + lt[i], header=True)
    #     df = df.withColumn("time_on", df["time_on"].astype("long"))
    #     func_null_to_zero = udf(time_c, StringType())
    #     df = df.withColumn("time_on", func_null_to_zero("time_on"))
    #     df = df.groupBy("time_on").count()
    #     print(lt[i])
    #     df.show(1000)

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
    # df.show(1000)
    # df.show(1000)
    # df = spark.read.csv("hdfs://43.58.5.249:8020/co_ap_list/1588183201.csv", header=True)
    # df_pla = spark.read.jdbc(url="jdbc:postgresql://43.58.5.248:5432/police_analysis_db",
    #                    table="zhaoqing_duanzhou_db.gd_device",
    #                    properties={'user': 'postgres', 'password': 'postgres'})
    # spark.read.csv("hdfs://192.168.7.150:8020/expansion_test/202009070959.csv", header=True).groupBy("mobile_mac").count().show(1000)
    # spark.read.parquet('hdfs://192.168.7.150:8020/data_sync_test/gd_ele_fence/202009110800').show()
    # a = [[1, 'ww,a', 333, 1], [1, 'ww,b', 333, 2], [3, 'ww', 556, 3], [3, 'ww', 556, 5], [3, 'ww', 556, 4], [2, 'ww', 333, 6], [2, 'ww', 333, 7]]
    # df = spark.createDataFrame(a, ['id', 'name', 'age', 'count'])
    # df2 = spark.createDataFrame(a, ['id', 'name', 'age', 'count']).limit(1)
    # df.union(df2).show()
    # client = pyhdfs.HdfsClient(hosts='192.168.7.150:9870', user_name='hdfs')
    # file_path_list = client.delete('/test/xkx/compress/audit_path/1606896512.5533059', recursive=True)
    import random
    # print(file_path_list)
    def udf_x1000(data):
        """某字段所有数字类型数据乘以1000"""
        return int(int(data) / 1000)
    # df = spark.range(10)
    # for i in range(len(file_list)):
    #     df = spark.read.parquet('hdfs://192.168.7.150:8020/data_etl/' + file_list[i])
    #     df.write.parquet('hdfs://192.168.7.150:8020/data_etl/' + file_list[i] + '_bak', mode='overwrite')
    #     df = spark.read.parquet('hdfs://192.168.7.150:8020/data_etl/' + file_list[i] + '_bak')
    #     # df = df.withColumnRenamed('netbar_wacode', 'place_code').withColumnRenamed('time_start', 'time_on').withColumnRenamed('time_end', 'time_off').withColumnRenamed('mobile_mac', 'mac').repartition(1)
    #     func_x1000 = udf(udf_x1000, LongType())
    #     df = df.withColumn("time_on", func_x1000("time_on")).withColumn("time_off", func_x1000("time_off"))
    #     df.show()
    #     df.write.parquet('hdfs://192.168.7.150:8020/data_etl/' + file_list[i], mode='overwrite')
    #     client.delete('/data_etl/' + file_list[i] + '_bak', recursive=True)

    #     break
    # df = spark.read.parquet('hdfs://192.168.7.150:8020/data_etl/20200709')

    # df.printSchema()


    # lt = client.listdir('/weihai/src/probe_type/20201213')
    # for i in range(len(lt)):
    #     df = spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/probe_type/20201213/' + lt[i])
    #     df.printSchema()
    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/compress/probe_type/20201205_test').printSchema()
    # lt = ['20201205', '20201206', '20201207', '20201208', '20201209', '20201210']
    # for i in range(len(lt)):
    #     df2 = spark.read.parquet('hdfs://192.168.7.150:8020/weihai/compress/wifi_type/' + lt[i])
    #     df2 = df2.withColumn("place_code", df2["place_code"].cast(StringType()))
    #     df2 = df2.repartition(1)
    #     df2.write.parquet('hdfs://192.168.7.150:8020/weihai/compress/wifi_type/' + lt[i] + '_test')

    # df.show()
    # df = df.withColumn("place_code", df["place_code"].cast(StringType()))
    # # df.printSchema()
    # df.sort(df['start_time'].desc()).show()

    # df.select('mac').dropDuplicates().show(100)
    # df.show()
    # df = df[df["probe_mac"] == 'F0-6D-78-4A-7D-7D']
    # df = df.sort(df['start_time'])
    # df = df[df["mac"] == 'F0-6D-78-4A-7D-7D']
    # df = df.select("count")
    # df = df.sort(df["count"].desc())
    # 压缩的最早2020-12-05 06:03:00
    # df = df.sort(df.start_time)
    # print(df.select('start_time').first()[0])
    # df = df.sort(df.start_time.desc())
    # print(df.select('start_time').first()[0])
    # print(df.count())
    # if not client.exists(''):
    #     client.mkdirs('')

    # path = 'hdfs://192.168.7.150:8020/weihai/compress/probe_type/'
    # lt = [path + i + '*' for i in lt]
    # df = spark.read.format('parquet').load(lt)
    # print(df.count())
    # print(spark.read.parquet(tuple(path+lt[0], path+lt[1])).count())

    # df1 = spark.read.parquet(path + lt[0])
    # df2 = spark.range(0)
    # df1 = df1.union(df2)
    # df1.show()

    # print(client.exists('/weihai/compress/probe_type/' + '20201205'))

    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/compress/wifi_type/20201205').printSchema()
    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/compress/probe_type/20201205').printSchema()
    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/audit_type/20201205_combine').printSchema()
    # def udf_day_format(time_data):
    #     return time.strftime("%Y%m%d", time.localtime(int(time_data / 1000)))
    # # # 造wifi_probe
    # schema_ = StructType([StructField("mac", StringType(), True),
    #                       StructField("place_code", StringType(), True),
    #                       StructField("time_on", LongType(), True),
    #                       StructField("time_off", LongType(), True),
    #                       StructField("count", LongType(), True),])
    # a = [['77-77-77-77-77-77', '37100221484860', 1606756138000, 1607101738000, 1000],
    #      ['77-77-77-77-77-77', '37100221327799', 1606756138000, 1607101738000, 1000],]
    # df = spark.createDataFrame(a, schema=schema_).repartition(1)
    # func_day_format = udf(udf_day_format, StringType())
    # time_field = "time_on"
    # spark_tmp = df.select(time_field).withColumn(time_field, func_day_format(time_field)).dropDuplicates()
    # lt = list(spark_tmp.toPandas()[time_field])
    # for i in range(len(lt)):
    #     time_start = int(time.mktime(time.strptime(lt[i], "%Y%m%d"))) * 1000
    #     time_end = time_start + 86400000
    #     print(time_start)
    #     print(time_end)

    # df.write.parquet('hdfs://192.168.7.150:8020/weihai/compress/wifi_type/20201201')
    # df.write.parquet('hdfs://192.168.7.150:8020/weihai/compress/probe_type/20201201')
    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/compress/wifi_type/20201201').printSchema()
    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/compress/probe_type/20201201').printSchema()

    # # 伪造audit
    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/audit_type/20201205_combine').printSchema()
    # schema = spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/audit_type/20201205_combine').schema
    # a = [['37100221484860', '127.0.0.1', 1606795738000, '77-77-77-77-77-77', '127.0.0.1', 1, 100, 'http://', '', '127.0.0.1', 'D4-EE-07-5C-14-76', '234118790780022', '11.1', '22.2', 7777, 1, 1, 1]]
    # df = spark.createDataFrame(a, schema=schema).repartition(1)
    # df.printSchema()
    # df.show()
    # lt = ['12', '13', '14', '15', '16', '17', '18', '19', '20', '21']
    # for i in range(len(lt)):
    #     print(lt[i])
    #     df = spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/audit_type/202012' + lt[i])
    #     df = df.groupBy(['dst_v4ip', 'place_code']).agg(functions.count("dst_v4ip"))
    #     df.sort(df['dst_v4ip']).show()

    day_list = ['hdfs://192.168.7.150:8020/weihai/compress/wifi_type/20201215', 'hdfs://192.168.7.150:8020/weihai/compress/wifi_type/20201216']
    # spark_df = spark.read.format('parquet').load(day_list)
    # spark_df.show()
    # spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/weihai/dst/audit_type/20201102').show()
    # spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/weihai/src/wifi_type/20201102').show()
    # spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/weihai/src/wifi_type/20201103').show()
    # spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/weihai/src/wifi_type/20201104').show()

    # spark.read.parquet('hdfs://192.168.7.150:8020/weihai_bak/src/probe_type/20170614').show()

    # day = '20201111'
    # print(time.strptime(day, "%Y%m%d"))
    # df = spark.read.csv('D:/code/cjh_study/test.csv', header=True, sep=",")
    # def f(d):
    #     d = d.replace('-', '')
    #     d = int(d, 16)
    #     return d
    # func_null_to_zero = udf(f, StringType())
    # df = df.withColumn("d", func_null_to_zero("d"))
    # df.show(4000)
    # df_ef = spark.range(2).withColumnRenamed('id', 'probe_mac').toPandas()
    # print(df_ef)
    # print(df_ef[df_ef['probe_mac'].isin([0])])
    # if not df_ef[df_ef['probe_mac'].isin([0])].empty:
    #     print(1)
    # schema_ = StructType([StructField("mac", StringType(), True),
    #                       StructField("place_code", StringType(), True),
    #                       StructField("time_on", LongType(), True),
    #                       StructField("time_off", LongType(), True),
    #                       StructField("count", LongType(), True),])
    # a = [['77-77-77-77-77-77', '37100221484860', 2, 1607101738000, 1000],
    #      ['77-77-77-77-77-77', '37100221327799', 1, 1607101738000, 1000],
    #      ['77-77-77-77-77-77', '37100221327799', 0, 1607101738000, 1000],
    #      ['77-77-77-77-77-77', '37100221327799', 32, 1607101738000, 1000],]
    # df = spark.createDataFrame(a, schema=schema_).repartition(1).toPandas()
    # print(df)
    # df = df.sort_values("time_on", ascending=False).reset_index().drop(["index"], axis=1)
    # print(df)

    # df = spark.read.parquet('hdfs://192.168.7.150:8020/weihai/src/probe_type/20201205')
    # df.printSchema()
    # df.show()
    # df[df['mac'] == 'EC-01-EE-37-B5-4E'].show()
    import nacos
    import yaml

    # SERVER_ADDRESSES = "192.168.1.99:8848"
    # NAMESPACE = 'dev'
    # client = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE, username="nacos", password="ggsddu@123")
    # data_id = "application-dev.yml"
    # group = "dev"
    # conf_nacos = client.get_config(data_id, group)
    # conf = yaml.load(conf_nacos)
    # print(conf)
    df1 = spark.range(3).toPandas()
    df2 = spark.range(5).toPandas()
    df = pd.concat([df1, df2], axis=1)
    print(df)