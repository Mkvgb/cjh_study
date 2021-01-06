# -*- encoding: utf-8 -*-
"""
@File       :   dev_pla_init.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/9/11 16:53
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
# -*- encoding: utf-8 -*-
import time
import json
# import paramiko
# from fastparquet import *
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Bar
# from com.ggsddu.utilscjh.pg_update_cjh import *

def create_batch_update_sql(scheme, table, data):
    id_list = list()
    case_str = ""
    for record in data:
        id_list.append(str(record['id']))
        case_str += f"when {str(record['id'])} then {record['count']}"
    data_range = ','.join(id_list)
    query = f"update {scheme}.{table} set count = case id {case_str} end where id in({data_range});"
    return query

def update_anxing_dev_pla_true():
    # [address, longitude, latitude, access_ip, device_mac]
    df_new = spark.read.csv("hdfs://192.168.7.150:8020/test/cjh/csv/axdp1.csv", header=True)
    func_h_2_d = udf(UdfUtils().udf_h_2_d, LongType())
    df_new = df_new.withColumn("device_mac", func_h_2_d("device_mac"))


    # # [netbar_wacode, device_mac, address, longitude, latitude, access_ip, device_mac]
    # df_update = df_ap.join(df_new, ["device_mac"], "left")

    df_dev = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/police_center_db", table="co_device_copy1", properties={'user': 'root', 'password': 'root123'})
    df_pla = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/police_center_db", table="pu_place", properties={'user': 'root', 'password': 'root123'})
    df_pla = df_pla.select("place_code", "name", "address", "longitude", "latitude", "access_ip")
    func_contain_XXX = udf(UdfUtils().udf_contain_XXX, StringType())
    df_pla = df_pla.withColumn("name", func_contain_XXX("name")).withColumn("address", func_contain_XXX("address"))
    # [netbar_wacode, collection_equipment_name, collection_equipment_address, longitude, latitude, access_ip]
    df_pla = df_pla[df_pla["name"] != "0"][df_pla["address"] != "0"].withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("name", "collection_equipment_name").withColumnRenamed("address", "collection_equipment_address")
    # [netbar_wacode, place_name, site_address, longitude, latitude, ip_addr]
    df_pla = df_pla.withColumnRenamed("access_ip", "ip_addr").withColumnRenamed("collection_equipment_name", "place_name").withColumnRenamed("collection_equipment_address", "site_address")
    func_copy = udf(UdfUtils().udf_copy, StringType())
    df_pla = df_pla.withColumn("place_name", func_copy("place_name"))
    # [netbar_wacode, place_name, site_address, longitude, latitude, ip_addr, device_name, create_time, update_time]
    df_pla = df_pla.withColumn("device_name", split("place_name", ",")[0]).withColumn("place_name", split("place_name", ",")[1]).withColumn("create_time", functions.lit(int(time.time() * 1000))).withColumn("update_time", functions.lit(int(time.time() * 1000)))

    df_o = df_new.join(df_pla, ["longitude", "latitude"], "left")
    func_null_2_zero = udf(UdfUtils().udf_null_2_zero, StringType())
    df_o = df_o.withColumn("netbar_wacode", func_null_2_zero("netbar_wacode"))
    df_o = df_o[df_o["netbar_wacode"] != "0"]
    func_mac = udf(UdfUtils().udf_mac_o2h, StringType())
    df_o = df_o.withColumn("device_mac", func_mac("device_mac"))
    df_dev = df_o.select("netbar_wacode", "place_name", "device_name", "ip_addr", "create_time", "update_time", "device_mac").withColumn("company_code", functions.lit("755652234"))
    df_pla = df_o.select("netbar_wacode", "place_name", "site_address", "longitude", "latitude", "create_time", "update_time").withColumn("company_code", functions.lit("755652234"))

    df_dev = df_dev.toPandas()
    df_dev_id = spark.range(66).toPandas()
    df_dev_id["id"] = df_dev_id["id"].map(lambda x: UdfUtils().udf_add(x, 12165))
    df_dev = pd.concat([df_dev, df_dev_id], axis=1, join='inner')
    df_pla = df_pla.toPandas()
    df_pla_id = spark.range(66).toPandas()
    df_pla_id["id"] = df_pla_id["id"].map(lambda x: UdfUtils().udf_add(x, 3098))
    df_pla = pd.concat([df_pla, df_pla_id], axis=1, join='inner')
    spark.createDataFrame(df_dev).write.mode(saveMode='append').jdbc(url="jdbc:postgresql://192.168.7.160:5432/police_analysis_db", table="zhaoqing_duanzhou_db.gd_device", properties={'user': 'postgres', 'password': 'postgres'})
    spark.createDataFrame(df_pla).write.mode(saveMode='append').jdbc(url="jdbc:postgresql://192.168.7.160:5432/police_analysis_db", table="zhaoqing_duanzhou_db.gd_place", properties={'user': 'postgres', 'password': 'postgres'})

def update_anxiang_pla_dev_false():
    df_dev = spark.read.jdbc(url="jdbc:postgresql://192.168.7.160:5432/police_analysis_db", table="zhaoqing_duanzhou_db.gd_device_copy1", properties={'user': 'postgres', 'password': 'postgres'})
    # df_pla = spark.read.jdbc(url="jdbc:postgresql://192.168.7.160:5432/police_analysis_db", table="zhaoqing_duanzhou_db.gd_place_copy1", properties={'user': 'postgres', 'password': 'postgres'})

    df_dev = df_dev.select("ip_addr")

    # [address, longitude, latitude, ip_addr, device_mac]
    df_new = spark.read.csv("hdfs://192.168.7.150:8020/test/cjh/csv/axdp1.csv", header=True).withColumnRenamed("access_ip", "ip_addr")

    df_new_ip = df_new.select("ip_addr")

    df_dev = df_new_ip.subtract(df_dev).join(df_new, ["ip_addr"], "left")

    # [place_name, longitude, latitude, ip_addr, device_mac, company_code, place_name]
    df_dev = df_dev.withColumn("company_code", functions.lit("755652234")).withColumnRenamed("address", "place_name")

    func_copy = udf(UdfUtils().udf_copy, StringType())
    df_dev = df_dev.withColumn("place_name", func_copy("place_name"))
    # [place_name, longitude, latitude, ip_addr, device_mac, company_code, place_name, device_name, create_time, update_time]
    df_dev = df_dev.withColumn("device_name", split("place_name", ",")[0]).withColumn("site_address", split("place_name", ",")[0]).withColumn("place_name", split("place_name", ",")[1]).withColumn("create_time", functions.lit(int(time.time() * 1000))).withColumn("update_time", functions.lit(int(time.time() * 1000)))
    df_dev.show()


    df_dev = df_dev.toPandas()
    df_dev_id = spark.range(137).withColumnRenamed("id", "id_dev").toPandas()
    df_dev_id["id_dev"] = df_dev_id["id_dev"].map(lambda x: UdfUtils().udf_add(x, 12231))
    df_dev = pd.concat([df_dev, df_dev_id], axis=1, join='inner')
    df_dev_pla = spark.range(137).withColumnRenamed("id", "netbar_wacode")
    df_dev_id2 = spark.range(137).withColumnRenamed("id", "id_pla").toPandas()
    df_dev_id2["id_pla"] = df_dev_id2["id_pla"].map(lambda x: UdfUtils().udf_add(x, 3164))
    df_dev = pd.concat([df_dev, df_dev_id2], axis=1, join='inner')
    # .withColumn("netbar_wacode", df_dev_pla["netbar_wacode"].astype("string"))
    df_dev_pla = df_dev_pla.toPandas()
    df_dev_pla["netbar_wacode"] = df_dev_pla["netbar_wacode"].map(lambda x: UdfUtils().udf_add(x, 11111115000000))
    df_dev = pd.concat([df_dev, df_dev_pla], axis=1, join='inner')

    df_dev = spark.createDataFrame(df_dev)

    df_dev.show(1000)

    df_dev_in = df_dev.select("id_dev", "netbar_wacode", "place_name", "device_name", "ip_addr", "create_time", "update_time", "device_mac", "company_code")
    func_h_2_d = udf(UdfUtils().udf_h_2_d, LongType())
    df_dev_in = df_dev_in.withColumn("device_mac", func_h_2_d("device_mac"))
    func_mac = udf(UdfUtils().udf_mac_o2h, StringType())
    df_dev_in = df_dev_in.withColumn("device_mac", func_mac("device_mac"))
    df_dev_in = df_dev_in[df_dev_in["device_mac"] != "C8-EE-A6-3F-01-A1"]
    df_dev_in.show(1000)

    df_pla_in = df_dev.select("id_pla", "netbar_wacode", "place_name", "site_address", "longitude", "latitude", "create_time", "update_time", "company_code")
    df_pla_in.show(1000)


    df_dev_in.withColumnRenamed("id_dev", "id").write.mode(saveMode='append').jdbc(url="jdbc:postgresql://192.168.7.160:5432/police_analysis_db", table="zhaoqing_duanzhou_db.gd_device", properties={'user': 'postgres', 'password': 'postgres'})
    df_pla_in.withColumnRenamed("id_pla", "id").write.mode(saveMode='append').jdbc(url="jdbc:postgresql://192.168.7.160:5432/police_analysis_db", table="zhaoqing_duanzhou_db.gd_place", properties={'user': 'postgres', 'password': 'postgres'})

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

    def udf_h_2_d(self, data):
        return int(data, 16)

    def udf_contain_XXX(self, data):
        if data.find("XXX"):
            return data
        else:
            return "0"

    def udf_copy(self, data):
        return data + ',' + data

    def udf_null_2_zero(self, data):
        if not data:
            return "0"
        else:
            return data

    def udf_add(self, data, add):
        return data + add

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
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # # sch = StructType([
    # #     StructField("address", StringType(), nullable=False), StructField("longitude", StringType()), StructField("latitude", StringType()), StructField("access_ip", StringType()), StructField("device_mac", LongType())
    # # ])
    #
    # # df = spark.read.csv("hdfs://192.168.7.150:8020/test/cjh/csv/axdp1.csv", header=True)
    # #
    # # func_h_2_d = udf(UdfUtils().udf_h_2_d, LongType())
    # # df = df.withColumn("device_mac", func_h_2_d("device_mac"))
    # #
    # # df.show(1000)
    #
    # sch_ap = StructType([
    #     StructField("id", LongType()), StructField("ap_mac", LongType()), StructField("str_ap_mac", StringType()), StructField("ssid", StringType()), StructField("signal", StringType()), StructField("channel", LongType()),
    #     StructField("encryption_type", LongType()), StructField("company", StringType()), StructField("time_on", LongType()), StructField("time_off", LongType()), StructField("time_update", LongType()), StructField("local_mac", LongType()),
    #     StructField("place_code", LongType())
    # ])
    #
    # df_ap = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/co_ap_list_1588528801.csv', schema=sch_ap)
    #
    # sch_ele = StructType([
    #     StructField("id", LongType()), StructField("time_on", LongType()), StructField("time_off", LongType()), StructField("time_update", LongType()), StructField("router_mac", LongType()), StructField("signal", StringType()), StructField("mobile_mac", LongType()), StructField("str_mobile_mac", StringType()), StructField("company", StringType()), StructField("place_code", LongType()), StructField("cache_ssid", StringType()), StructField("identification_type", StringType()), StructField("cretificate_code", StringType()), StructField("ssid_position", StringType()), StructField("access_ap_mac", StringType()), StructField("access_ap_channel", StringType()), StructField("access_ap_encryption_type", StringType()), StructField("x_coordinate", StringType()), StructField("y_coordinate", StringType()), StructField("collection_equipment_id", StringType()), StructField("collection_equipment_longitude", StringType()), StructField("collection_equipment_latitude", StringType()),
    # ])
    #
    # # df_ele = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/co_ele_fence_1590170403.csv', schema=sch_ele)
    #
    # df_ap = df_ap.select("local_mac", "place_code").dropDuplicates().withColumnRenamed("local_mac", "device_mac").withColumnRenamed("place_code", "netbar_wacode")
    #


    # df = df_update.toPandas()
    # if not df.empty:
    #     t_data = df.to_dict(orient="records")
    #     query = create_batch_update_sql("zhaoqing_duanzhou_db", "collection_equipment_name", t_data, 'device_total', "int")
    #     with PGSQLOpr("192.168.1.99", 3306, "police_center_db", "root", "root123") as opr:
    #         opr.update_cursor()
    #         opr.cursor.execute(query)
    #         opr.conn.commit()

    # spark.read.parquet("hdfs://192.168.7.150:8020/data_sync_test/gd_ele_fence/newest/0.snappy.parquet").show()

    df_ele_fence = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/ele_test.csv', header=True)
    df_dev = df_ele_fence.withColumnRenamed("local_mac", "device_mac").withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("router_mac", "device_mac").select("netbar_wacode", "device_mac").dropDuplicates(["netbar_wacode"])
    df_dev = df_dev.withColumn("netbar_wacode", df_dev["netbar_wacode"].astype("string")).withColumn("device_mac", df_dev["device_mac"].astype("long"))
    func_mac_o2h = udf(UdfUtils().udf_mac_o2h, StringType())
    df_dev = df_dev.withColumn("device_mac", func_mac_o2h("device_mac"))
    df_dev.show()
    # df_track_append = df_track_append.join(df_dev, ["netbar_wacode"], "left")

    # 潺陵路图书馆门口   \    4组—潺陵东路-长寿巷口2     C8-EE-A6-3F-01-A1

    # 11111115000000

    # [netbar_wacode, place_name, site_address, longitude, latitude, ip_addr, device_name, create_time, update_time]

    spark.stop()
