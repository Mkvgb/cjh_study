# -*- encoding: utf-8 -*-
"""
@File       :   base_cjh.py
@Contact    :   suntang.com
@Modify Time:   2020/8/27 17:19
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import pandas as pd
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions
from io import StringIO
from sqlalchemy import create_engine
import psycopg2


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
        self.PGSQL_PROPERTIES = {'user': 'postgres', 'password': 'postgres'}

    def cjhtest(self):
        pass


class UdfUtils(object):
    def udf_x1000(self, data):
        """某字段所有数字类型数据乘以1000"""
        return int(data) * 1000


if __name__ == '__main__':
    # print("======1:" + str(time.asctime(time.localtime(time.time()))))
    conf = SparkConf().setAppName('cjh')
    # conf.set("spark_cjh.driver.memory", "8g")
    # conf.set("spark_cjh.executor.memory", "8g")
    # conf.set("spark_cjh.driver.memory", "2g")
    # conf.set("spark_cjh.executor.memory", "8g")
    # conf.set("spark_cjh.num.executors", "3")
    # conf.set("spark_cjh.executor.core", "3")
    # conf.set("spark_cjh.default.parallelism", "20")
    # conf.set("spark_cjh.memory.fraction", "0.75")
    # conf.set("spark_cjh.memory.storageFraction", "0.75")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # CjhTest(spark).cjhtest()

    # df = spark.range(10).toPandas()

    a = [[1, 'ww', 333, 4], [2, 'ww', 333, 3], [3, 'ww', 556, 4], [4, 'ww', 556, 5], [5, 'ww', 556, 4], [6, 'ww', 333, 6], [7, 'ww', 333, 7]]
    dfa = spark.createDataFrame(a, ["id", "name", "age", "count"]).sort("count")
    # df = dfa.toPandas()

    # # df写入
    # engine = create_engine(
    #     "postgresql://postgres:postgres@192.168.1.99:6543/postgres",
    #     max_overflow=0,  # 超过连接池大小外最多创建的连接
    #     pool_size=5,  # 连接池大小
    #     pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
    #     pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
    # )
    # sio = StringIO()
    # df.to_csv(sio, sep='|', index=False)
    # pd_sql_engine = pd.io.sql.pandasSQL_builder(engine)
    # table = 'atest'
    # schema = 'analysis_etl_gd_ele_fence'
    # pd_table = pd.io.sql.SQLTable(table, pd_sql_engine, frame=df, index=False, if_exists="append",
    #                               schema=schema)
    # pd_table.create()
    # sio.seek(0)
    # with engine.connect() as connection:
    #     with connection.connection.cursor() as cursor:
    #         copy_cmd = f"COPY {schema}.{table} FROM STDIN HEADER DELIMITER '|' CSV"
    #         cursor.copy_expert(copy_cmd, sio)
    #     connection.connection.commit()

    # # pgsql update
    # t_data = df.to_dict(orient="records")
    # query = create_batch_update_sql("analysis_etl_gd_ele_fence", "atest", t_data)
    # with PGSQLOpr("192.168.1.99", 6543, "postgres", "postgres", "postgres") as opr:
    #     opr.update_cursor()
    #     opr.cursor.execute(query)
    #     opr.conn.commit()


    # df = spark.read.jdbc(url="jdbc:postgresql://192.168.1.99:6543/postgres", table="analysis_etl_gd_ele_fence.gd_device", properties={'user': 'postgres', 'password': 'postgres'})
    # df = df.select("id")
    # df = df.withColumn("update_time", functions.lit(1))
    # df.write.jdbc(url="jdbc:postgresql://192.168.1.99:6543/postgres", table="analysis_etl_gd_ele_fence.atest2", properties={'user': 'postgres', 'password': 'postgres'})
    # spark.stop()

    df = spark.range(10)
    # df.write.jdbc(url="jdbc:postgresql://192.168.1.99:6543/postgres", table="analysis_etl_gd_ele_fence.atest3", properties={'user': 'postgres', 'password': 'postgres'}, mode="append")