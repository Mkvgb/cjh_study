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


class Analysis_anxiang_data(object):
    def __init__(self, spark):
        self.spark = spark

    def dev_pla_2pgsql(self):
        """将10个表的场所与设备放入pgsql中"""
        lt = [1587751202, 1587837602, 1588183201, 1588269601, 1588356001, 1588442401, 1588442402, 1588528801, 1588615201, 1588701601]
        for i in range(len(lt)):
            df_ap = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/police_center_db", table="co_ap_list_" + str(lt[i]), properties={'user': 'root', 'password': 'root123'})
            print("co_ap_list_" + str(lt[i]) + " count : " + str(df_ap.count()))
            df_ap = df_ap.select("local_mac", "place_code").dropDuplicates()
            if i != 0:
                df_tmp = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="pla_dev", properties={'user': 'root', 'password': 'root123'})
                df_tmp.write.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="pla_dev_tmp", properties={'user': 'root', 'password': 'root123'}, mode="overwrite")
                df_tmp = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="pla_dev_tmp", properties={'user': 'root', 'password': 'root123'})
                df_ap = df_ap.union(df_tmp).dropDuplicates()
            df_ap.write.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="pla_dev", properties={'user': 'root', 'password': 'root123'}, mode="overwrite")

    def plot_mac_count_perpla(self):
        """将每个place在每个表中探测到mac的个数，得先运行上面的代码将数据存入pgsql中（集群），在运行下面的进行绘图（本地）"""
        lt = [1588528801, 1588615201, 1588701601]
        for i in range(len(lt)):
            # # pla与mac_count放入表中
            # df_ap = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/police_center_db", table="co_ap_list_" + str(lt[i]), properties={'user': 'root', 'password': 'root123'})
            # df = df_ap.select("local_mac", "ap_mac").dropDuplicates().groupBy("local_mac").count()
            # df.write.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="co_ap_list_" + str(lt[i]) + "_maccount", properties={'user': 'root', 'password': 'root123'}, mode="overwrite")

            df = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="co_ap_list_" + str(lt[i]) + "_maccount", properties={'user': 'root', 'password': 'root123'})
            lt_local_mac = df.select("local_mac").toPandas().values.tolist()
            lt_count = df.select("count").toPandas().values.tolist()
            lt_local_mac = [x[0] for x in lt_local_mac]
            lt_count = [x[0] for x in lt_count]
            sns.barplot(lt_local_mac, lt_count, palette=sns.color_palette('YlGn'))
            plt.title("table : " + str("co_ap_list_" + str(lt[i]) + "_maccount"))
            plt.xticks(rotation=90)
            plt.show()

    def a_mac_per_pla_2pgsql(self):
        """将指定ap_mac在所有场所出现的次数进行统计并入pgsql表中"""
        sch = StructType([
            StructField("id", LongType()), StructField("ap_mac", LongType()), StructField("str_ap_mac", StringType()), StructField("ssid", StringType()), StructField("signal", StringType()), StructField("channel", LongType()),
            StructField("encryption_type", LongType()), StructField("company", StringType()), StructField("time_on", LongType()), StructField("time_off", LongType()), StructField("time_update", LongType()),
            StructField("local_mac", LongType()),
            StructField("place_code", LongType())
        ])
        df = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/co_ap_list_1588528801.csv', schema=sch)
        df = df.select("ap_mac", "local_mac")
        df = df[df["ap_mac"] == 50671505619308].groupBy("local_mac").count().withColumn("ap_mac", functions.lit(50671505619308))
        df.write.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="a_mac_perpla", properties={'user': 'root', 'password': 'root123'}, mode="append")

    def polt_a_mac_per_pla(self):
        """得先运行a_mac_per_pla_2pgsql，将多个ap_mac放入表中
            后续对某一ap_mac在一个表中被local_mac探测到的次数进行统计并绘图"""
        df = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="a_mac_perpla", properties={'user': 'root', 'password': 'root123'})
        iter_dev = df.select('ap_mac').dropDuplicates().toLocalIterator()
        for index_dev in iter_dev:
            df_dev = df[df["ap_mac"] == index_dev.ap_mac].select('local_mac', 'count')
            dev_list = df_dev.select('local_mac').toPandas().values.tolist()
            mac_count_list = df_dev.select('count').toPandas().values.tolist()
            dev_list = [x[0] for x in dev_list]
            mac_count_list = [x[0] for x in mac_count_list]
            sns.barplot(dev_list, mac_count_list, palette=sns.color_palette('YlGn'))
            plt.title("ap_mac : " + str(index_dev.ap_mac) + "  (x-local_mac, y-count)")
            plt.xticks(rotation=90)
            plt.show()

    def spc_mobile_track(self, mobile_mac_list):
        """将指定mobile_mac的time_on,mobile_mac,netbar_wacode,dev_mac入库，以供后续错误分析"""
        sch = StructType([
            StructField("id", LongType()), StructField("ap_mac", LongType()), StructField("str_ap_mac", StringType()), StructField("ssid", StringType()), StructField("signal", StringType()), StructField("channel", LongType()),
            StructField("encryption_type", LongType()), StructField("company", StringType()), StructField("time_on", LongType()), StructField("time_off", LongType()), StructField("time_update", LongType()),
            StructField("local_mac", LongType()),
            StructField("place_code", LongType())
        ])
        df = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/co_ap_list_1588528801.csv', schema=sch)
        df = df.select('time_on', 'ap_mac', 'place_code', 'local_mac').withColumnRenamed("ap_mac", "mobile_mac").withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("local_mac", "collection_equipment_mac")
        # df = df.select('time_on', 'mobile_mac', 'place_code', 'router_mac').withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("router_mac", "collection_equipment_mac")

        df_ele_fence = df
        for i in range(len(mobile_mac_list)):
            df_mobile_mac = df_ele_fence[df_ele_fence["mobile_mac"] == mobile_mac_list[i]]
            df_i = df_mobile_mac
            iter_place = df_i.select('netbar_wacode').dropDuplicates().toLocalIterator()
            min = df_i.select("time_on").groupBy().min('time_on').first()[0]
            max = df_i.select("time_on").groupBy().max('time_on').first()[0]
            span = 60
            min = int(min / span) * span
            max = int(max / span) * span
            _span = int(max / span - min / span) + 1
            zero_list, time_list, u_list = [], [], []
            schema = StructType([
                StructField("time_on", LongType()),
                StructField("tome_on_count", LongType())
            ])
            for j in range(_span):
                time_list.append(min + j * span)
                u_list.append([min + j * span, 0])
            zero_df = self.spark.createDataFrame(u_list, schema)
            zero_df.cache()
            func = udf(lambda x: UdfUtils().udf_rounded_up(x, span), LongType())
            for index_dev in iter_place:
                df_time = df_i[df_i["netbar_wacode"] == index_dev.netbar_wacode].select("time_on")
                df_time = df_time.withColumn("time_on", func("time_on"))
                df_time = df_time.withColumn("count", functions.lit(1)).unionAll(zero_df)
                df_time = df_time.groupBy("time_on").sum("count").withColumnRenamed("sum(count)", "count").sort(
                    df_time.time_on)
                df_time = df_time.withColumn("netbar_wacode", functions.lit(index_dev.netbar_wacode))
                df_time = df_time.withColumn("mobile_mac", functions.lit(mobile_mac_list[i]))
                _table = "analysis_etl_gd_ele_fence.spe_mobile_mac_time_on_count"
                df_time.write.mode(saveMode='append').jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="a_track", properties={'user': 'root', 'password': 'root123'})

    def plot_spe_mobile_mac_timetrack(self, mobile_mac_list):
        """得先运行spec_mobile_mac_time_on_count2pgsql
        """
        _table = "analysis_etl_gd_ele_fence.spe_mobile_mac_time_on_count"
        df = self.spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="a_track", properties={'user': 'root', 'password': 'root123'})
        for i in range(len(mobile_mac_list)):
            df_i = df[df["mobile_mac"] == mobile_mac_list[i]].drop("mobile_mac")
            iter_place = df_i.select('netbar_wacode').dropDuplicates().toLocalIterator()
            time_df = df_i.select("time_on").dropDuplicates().sort("time_on")
            print(time_df.first()[0])
            _min = time_df.first()[0]
            time_list = time_df.toPandas().values.tolist()
            time_list = [x[0] for x in time_list]
            bar = (
                Bar()
                    .add_xaxis([(x - _min) for x in time_list])
                    .set_global_opts(
                    title_opts=opts.TitleOpts(title="timetrack",
                                              subtitle="time_on : " + str(time_list[0]) + " - " + str(time_list[-1])),
                    datazoom_opts=opts.DataZoomOpts())
            )
            for index_place in iter_place:
                df_track = df_i[df_i["netbar_wacode"] == index_place.netbar_wacode].select("time_on", "count").sort(
                    "time_on")
                mac_in_place_track_count_list = df_track.select("count").toPandas().values.tolist()
                mac_in_place_track_count_list = [x[0] for x in mac_in_place_track_count_list]
                bar.add_yaxis(index_place.netbar_wacode, mac_in_place_track_count_list)
            bar.render(r"D:\ap_mac_" + str(mobile_mac_list[i]) + ".html")

    def got_mobile_mac_freq(self):
        """横坐标是mobile被探测到的次数，纵坐标是这个被探测到的次数的个数
        具体是全部，跨度是50"""
        # url = self.DATA_20200711
        # df = SparkEleFenceAnalysis(self.spark).parquet_read(url).repartition(3)
        # df = spark.read.jdbc(url=MYSQL_URL, table="co_ele_fence", properties=MYSQL_PROPERTIES)
        sch = StructType([
            StructField("id", LongType()), StructField("ap_mac", LongType()), StructField("str_ap_mac", StringType()), StructField("ssid", StringType()), StructField("signal", StringType()), StructField("channel", LongType()),
            StructField("encryption_type", LongType()), StructField("company", StringType()), StructField("time_on", LongType()), StructField("time_off", LongType()), StructField("time_update", LongType()),
            StructField("local_mac", LongType()),
            StructField("place_code", LongType())
        ])
        df = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/co_ap_list_1588528801.csv', schema=sch)
        df = df.withColumnRenamed("ap_mac", "mobile_mac")
        df_count = df.select("mobile_mac").groupBy("mobile_mac").count().select("count")
        func = udf(UdfUtils().udf_rounded_up, LongType())
        func_rounded_up = udf(lambda x: UdfUtils().udf_rounded_up(x, 50), LongType())
        df_count = df_count.withColumn("count", func_rounded_up("count")).withColumnRenamed("count", "mac_count")
        df_count = df_count.groupBy("mac_count").count().withColumnRenamed("count", "mac_count_count")

        df_count = df_count[df_count["mac_count"] <= 5000]

        _table = "analysis_etl_gd_ele_fence.mac_count_count"
        df_count.write.mode(saveMode='overwrite').jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="a_fre", properties={'user': 'root', 'password': 'root123'})

    def plot_mobile_mac_freq(self):
        """得先运行got_mobile_mac_freq
        将上述got_mobile_mac_freq和got_mobile_mac_freq1的可视化
        具体是全部"""
        _table = "analysis_etl_gd_ele_fence.mac_count_count"
        df = self.spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="a_fre", properties={'user': 'root', 'password': 'root123'})
        df = df.sort("mac_count")
        list_mac_count = df.select("mac_count").toPandas().values.tolist()
        list_mac_count = [x[0] for x in list_mac_count]
        list_mac_count_count = df.select("mac_count_count").toPandas().values.tolist()
        list_mac_count_count = [x[0] for x in list_mac_count_count]
        sns.barplot(list_mac_count, list_mac_count_count, palette=sns.color_palette('YlGn'))
        plt.title("mobile_mac freq")
        plt.xticks(rotation=90)
        plt.show()
        bar = (
            Bar()
                .add_xaxis(list_mac_count)
                .add_yaxis("A", list_mac_count_count)
                .set_global_opts(
                title_opts=opts.TitleOpts(title="mac_count_count"),
                datazoom_opts=opts.DataZoomOpts())
        )
        bar.render(r"D:\test.html")

class CjhTest(object):
    def __init__(self, spark):
        self.spark = spark
        self.PGSQL_URL1 = "jdbc:postgresql://192.168.1.99:6543/postgres"
        self.HDFS_ST_PLACE_INFO = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place_info'
        self.PGSQL_PROPERTIES = {'user': 'postgres', 'password': 'postgres'}

    def cjhtest(self):
        pass

class UdfUtils(object):
    def udf_x1000(self, data):
        """某字段所有数字类型数据乘以1000"""
        return int(data) * 1000

    def udf_rounded_up(self, time_on, span):
        """按指定间隔舍入数据"""
        tmp = span / 2
        if time_on % span > tmp:
            return int(int(time_on / span) * span + span)
        elif time_on % span <= tmp:
            return int(int(time_on / span) * span)

if __name__ == '__main__':
    # print("======1:" + str(time.asctime(time.localtime(time.time()))))
    conf = SparkConf().setAppName('cjh')
    conf.set("spark.executor.heartbeatInterval", "200000")
    conf.set("spark.network.timeout", "300000")
    conf.set("spark_cjh.driver.memory", "8g")
    conf.set("spark_cjh.executor.memory", "8g")
    conf.set("spark_cjh.driver.memory", "2g")
    conf.set("spark_cjh.executor.memory", "8g")
    conf.set("spark_cjh.num.executors", "3")
    conf.set("spark_cjh.executor.core", "3")
    conf.set("spark_cjh.default.parallelism", "20")
    conf.set("spark_cjh.memory.fraction", "0.75")
    conf.set("spark_cjh.memory.storageFraction", "0.75")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    sch = StructType([
        StructField("id", LongType()), StructField("ap_mac", LongType()), StructField("str_ap_mac", StringType()), StructField("ssid", StringType()), StructField("signal", StringType()), StructField("channel", LongType()),
        StructField("encryption_type", LongType()), StructField("company", StringType()), StructField("time_on", LongType()), StructField("time_off", LongType()), StructField("time_update", LongType()), StructField("local_mac", LongType()),
        StructField("place_code", LongType())
    ])
    df_ap = spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/co_ap_list_1588528801.csv', schema=sch)

    # df = spark.read.jdbc(url="jdbc:mysql://192.168.1.99:3306/test", table="a_mac_perpla", properties={'user': 'root', 'password': 'root123'})

    lt1 = [28864944967802, 141352165890711, 88029466511761, 141352165895549, 123581278021554, 141352165895523, 26665921712232]

    # Analysis_anxiang_data(spark).spc_mobile_track(lt1)

    Analysis_anxiang_data(spark).plot_spe_mobile_mac_timetrack(lt1)

    # Analysis_anxiang_data(spark).got_mobile_mac_freq()

    # Analysis_anxiang_data(spark).plot_mobile_mac_freq()

    spark.stop()
