# -*- encoding: utf-8 -*-
"""
@File       :   base.py    
@Contact    :   suntang.com
@Modify Time:   2020/8/27 18:19
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import json
# import paramiko
from fastparquet import *
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions

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

    df = spark.read.json("/var/anxiang_data/thrid_20200828/renzixing/CSZL/20200721/00/30/20200721003301100_145_441200_723005104_008.log")
    df.show()

    spark.stop()
