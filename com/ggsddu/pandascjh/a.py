# -*- encoding: utf-8 -*-
"""
@File       :   st_etl_gd_ele_fence.py
@Contact    :   ggsddu.com
@Modify Time:   2020/8/21 11:19
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import json
import sys
import time
import pyhdfs
from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql import functions
from pyspark.sql.functions import udf, split, explode, concat_ws
from pyspark.sql.types import *


class AnalysisEleFence(object):
    def __init__(self, spark):
        self.spark = spark
        self.PGSQL_URL_analysis = "jdbc:postgresql://192.168.7.160:5432/police_analysis_db"
        self.PGSQL_PROPERTIES = {'user': 'postgres', 'password': 'postgres'}
        self.PGSQL_TABLE_ATTR = "zhaoqing_duanzhou_db.attr"
        self.PGSQL_TABLE_GD_PLACE = 'zhaoqing_duanzhou_db.gd_place'
        self.HDFS_ST_TRACK = 'hdfs://192.168.7.150:8020/track/tmp_track/st_track'
        self.HDFS_ST_TRACK_SUB = "/track/tmp_track/st_track"
        self.HDFS_ST_TRACK_INFO = 'hdfs://192.168.7.150:8020/track/tmp_track/st_track_info'
        self.HDFS_ST_TRACK_INFO_SUB = '/track/tmp_track/st_track_info'
        self.PYHDFS_HOST = '192.168.7.150:9870'
        self.HDFS_ST_DEVICE = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device'
        self.HDFS_ST_DEVICE_SUB = "/test/cjh/dev_pla/st_device"
        self.PGSQL_TABLE_GD_DEVICE = 'zhaoqing_duanzhou_db.gd_device'
        self.PGSQL_TABLE_TRACK = "zhaoqing_duanzhou_db.track"
        self.PGSQL_TABLE_ATTR_RECORD = "zhaoqing_duanzhou_db.attr_record"

    def etl_real_time(self, file_path, precision_span, timestack_span):
        """对所有的mobile_mac进行清洗，并入库"""
        # 读取实时拉取到的数据
        # ele[id,time_on,time_off,last_update_time,signal_strength,mobile_mac,company,hotpos_position,hotpos_ap_mac,hotpos_ap_channel,hotpos_tencrypt_type,
        #     x_coordinate,y_coordinate,equipment_code,equipment_longitude,equiment_latitude,netbar_wacode,collection_equipment_mac,status]
        df_ele_fence = self.spark.read.parquet(file_path)
        # df_ele_fence = self.spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/ap_test.csv', header=True)
        # df_ele_fence = self.spark.read.jdbc(url="jdbc:postgresql://192.168.1.99:6543/postgres", table="analysis_etl_gd_ele_fence.test_data_copy1", properties=self.PGSQL_PROPERTIES)
        etl_job = AnalysisEleFence(self.spark)
        # ele[time_on,mobile_mac,netbar_wacode,collection_equipment_mac]  --  prepare_data
        df_ele_fence = etl_job.prepare_data(df_ele_fence)
        # ele[mobile_mac, netbar_wacode, time_start, time_end, count]  --  etl_2_track
        df_ele_fence = etl_job.etl_2_track(df_ele_fence, precision_span, timestack_span)
        # 当前旧轨迹表数据从atrack表中取，并转存到atrack_tmp中，最后存回atrack，后续得改为到hdfs中取
        df_track_source = etl_job.union_track_2_hdfs(df_ele_fence, timestack_span)
        df_track_output = etl_job.standardize_etl_data(df_track_source)
        df_track_append = etl_job.update_track_talbe(df_track_output, file_path)
        etl_job.update_attr_record(df_track_append)

    def prepare_data(self, df_ele_fence):
        """数据准备工作，为正式处理前进行数据过滤，只操作attr表中有的mobile_mac，并选取后续所需字段"""
        df_ele_fence = df_ele_fence.withColumnRenamed("ap_mac", "mobile_mac").withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("local_mac", "collection_equipment_mac").withColumnRenamed("router_mac", "collection_equipment_mac")
        # 读入新拉取的gd_ele_fence信息
        func_mac_o2h = udf(UdfUtils().udf_mac_o2h, StringType())
        # mobile改为十六进制格式
        df_ele_fence = df_ele_fence.withColumn("mobile_mac", func_mac_o2h("mobile_mac"))
        # 指筛选attr中有的moblice_mac再进行etl处理
        df_attr = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_ATTR, properties=self.PGSQL_PROPERTIES)
        df_attr = df_attr[df_attr["attr_type_id"] == 5]
        df_attr_tmp = df_attr.select("attr_value").withColumnRenamed("attr_value", "mobile_mac")
        # 只选出attr表中有的mobile_mac进行处理
        df_ele_fence = df_attr_tmp.join(df_ele_fence, ["mobile_mac"], "left")
        func_null_to_zero = udf(UdfUtils().udf_null_to_zero, StringType())
        df_ele_fence = df_ele_fence.withColumn("netbar_wacode", func_null_to_zero("netbar_wacode"))
        # 去除掉netbar_wacode为null的数据
        df_ele_fence = df_ele_fence[df_ele_fence["netbar_wacode"] != "0"]
        # ele[time_on,mobile_mac,netbar_wacode,collection_equipment_mac]
        df_ele_fence = df_ele_fence.select('time_on', 'mobile_mac', 'netbar_wacode', 'collection_equipment_mac')
        df_ele_fence = df_ele_fence.withColumn("time_on", df_ele_fence["time_on"].astype("long")).withColumn("netbar_wacode", df_ele_fence["netbar_wacode"].astype("string")).withColumn("collection_equipment_mac", df_ele_fence["collection_equipment_mac"].astype("long"))
        return df_ele_fence

    def etl_2_track(self, df_ele_fence, precision_span, timestack_span):
        """对实时获取到的电子围栏数据进行清洗，形成轨迹，具体格式为[mobile_mac, netbar_wacode, time_start, time_end, count]"""
        # 将time_on精度调整为一分钟
        func_rounded_up = udf(lambda x: UdfUtils().udf_rounded_up(x, precision_span), LongType())
        df_ele_fence = df_ele_fence.withColumn("time_on", func_rounded_up("time_on"))

        # 找到每个mobile_mac在某个time_on被探测到最大次数时所在的场所，并保留此探测次数
        # ele[time_on,mobile_mac,netbar_wacode,collection_equipment_mac]
        df_ele_fence = df_ele_fence.groupBy(["mobile_mac", "netbar_wacode", "time_on"]).agg(functions.count("time_on")).withColumnRenamed("count(time_on)", "count")
        df_ele_fence = df_ele_fence.groupBy(["mobile_mac", "netbar_wacode", "time_on"]).max("count").withColumnRenamed("max(count)", "max_count")  # .sort("time_on") # 这里sort没用
        # [mobile_mac, netbar_wacode, time_on, max_count]
        df_ele_fence = df_ele_fence.withColumn("max_count", df_ele_fence.max_count.astype("string"))
        df_ele_fence = df_ele_fence.withColumn("time_on", df_ele_fence.time_on.astype("string"))

        # 将time_on和max_count合并
        # ele[mobile_mac, netbar_wacode, time_on(time_on, max_count)]
        df_ele_fence = df_ele_fence.select(concat_ws(',', df_ele_fence.time_on, df_ele_fence.max_count).alias('time_on'), "mobile_mac", "netbar_wacode")
        func_2_list_string = udf(UdfUtils().udf_2_list_string, StringType())
        # 为time_on(time_on, max_count)加上[]
        df_ele_fence = df_ele_fence.withColumn("time_on", func_2_list_string("time_on"))

        # 将[time_on, max_count]按mobile_mac和netbar_wacode分组，并合成list
        # 将相同mobile_mac和netbar_wacode的time_on(time_on, max_count)合并成list，具体为time_on = [[time_on,max_count],[time_on,max_count],...]
        df_ele_fence = df_ele_fence.groupBy(["mobile_mac", "netbar_wacode"]).agg(functions.collect_list('time_on').alias('time_list'))  # .drop("max(count)")
        # 删除没用的数据并按一定规则生成轨迹（开始时间+结束时间）
        func_etl = udf(lambda x: UdfUtils().udf_time_etl(x, timestack_span), StringType())
        df_ele_fence = df_ele_fence.withColumn("time_list", func_etl("time_list"))
        # explode成多个轨迹，并将轨迹拆分成time_start、time_end、count
        # ele[mobile_mac, netbar_wacode, time_list], 其中time_list = 12,13,1]', '[13,14,2]', '[14,15,3
        df_ele_fence = df_ele_fence.withColumn("time_list", explode(split("time_list", "\\]', '\\[")))
        # ele[mobile_mac, netbar_wacode, time_start, time_end, count]
        df_ele_fence = df_ele_fence.withColumn("time_start", split("time_list", ",")[0]).withColumn("time_end", split("time_list", ",")[1]).withColumn("count", split("time_list", ",")[2]).drop("time_list")
        df_ele_fence = df_ele_fence.withColumn("time_start", df_ele_fence.time_start.astype("long"))
        func_text2long = udf(UdfUtils().udf_text2long, LongType())
        df_ele_fence = df_ele_fence.withColumn("time_end", func_text2long("time_end"))
        df_ele_fence = df_ele_fence.withColumn("count", func_text2long("count"))
        return df_ele_fence

    def union_track_2_hdfs(self, df_ele_fence, timestack_span):
        """将hdfs中的旧track数据与新的track数据进行合并，进行轨迹合并操作，将合并后的新的轨迹存回hdfs"""
        # 读取旧track表中的内容，格式为[mobile_mac, netbar_wacode, time_start, time_end, count]
        # df_track = self.spark.read.jdbc(url=self.PGSQL_URL1, table="analysis_etl_gd_ele_fence.atrack", properties=self.PGSQL_PROPERTIES)
        # df_track.write.jdbc(url=self.PGSQL_URL1, table="analysis_etl_gd_ele_fence.atrack_tmp", properties=self.PGSQL_PROPERTIES, mode="overwrite")
        # df_track = self.spark.read.jdbc(url=self.PGSQL_URL1, table="analysis_etl_gd_ele_fence.atrack_tmp", properties=self.PGSQL_PROPERTIES)
        #
        # df_ele_fence = df_ele_fence.unionAll(df_track)
        client = pyhdfs.HdfsClient(hosts=self.PYHDFS_HOST, user_name='hdfs')


        # ele[mobile_mac, netbar_wacode, time_start, time_end, count]
        # 如果旧轨迹表有数据，则拉过来进行union并在后面进行轨迹合并
        # if client.exists("/track/tmp_track/st_track"):
        if client.exists(self.HDFS_ST_TRACK_SUB):
            # [mobile_mac, netbar_wacode, time_start, time_end, count]
            df_track = self.spark.read.csv(self.HDFS_ST_TRACK, header=True)
            df_track.write.csv(self.HDFS_ST_TRACK + '_tmp', header=True, mode='overwrite')
            df_track = self.spark.read.csv(self.HDFS_ST_TRACK + '_tmp', header=True)
            df_ele_fence = df_ele_fence.unionAll(df_track)

        df_ele_fence = df_ele_fence.sort("time_start")
        # ele[mobile_mac, netbar_wacode, time_on(time_start, time_end, count)]
        df_ele_fence = df_ele_fence.select(concat_ws(',', df_ele_fence.time_start, df_ele_fence.time_end, df_ele_fence["count"]).alias('time_on'), "mobile_mac", "netbar_wacode")
        func_2_list_string = udf(UdfUtils().udf_2_list_string, StringType())
        df_ele_fence = df_ele_fence.withColumn("time_on", func_2_list_string("time_on"))
        # ele[mobile_mac, netbar_wacode, time_list[time_on(time_start, time_end, count), time_on(time_start, time_end, count), ...]
        df_ele_fence = df_ele_fence.groupBy(["mobile_mac", "netbar_wacode"]).agg(functions.collect_list('time_on').alias('time_list'))
        func_track_union = udf(lambda x: UdfUtils().udf_track_union(x, timestack_span), StringType())
        # ele[mobile_mac, netbar_wacode, time_list[time_on(time_start, time_end, count), time_on(time_start, time_end, count), ...]
        # 将时间相差小于timestack_span就将两time_on合并
        df_ele_fence = df_ele_fence.withColumn("time_list", func_track_union("time_list"))

        # 将字段time_list中的列表数据explode，time_list成为一个一个轨迹
        # ele[mobile_mac, netbar_wacode, time_list[time_on(time_start, time_end, count)]
        df_ele_fence = df_ele_fence.withColumn("time_list", explode(split("time_list", "\\], \\[")))
        func_delete_space = udf(lambda x: UdfUtils().udf_delete_string_elemt(x, " "), StringType())
        # func_delete_quo = udf(lambda x: UdfUtils().udf_delete_string_elemt(x, "'"), StringType())
        df_ele_fence = df_ele_fence.withColumn("time_list", func_delete_space("time_list"))  # .withColumn("time_list", func_delete_quo("time_list"))
        # ele[mobile_mac, netbar_wacode, time_start, time_end, count]
        df_ele_fence = df_ele_fence.withColumn("time_start", split("time_list", ",")[0]).withColumn("time_end", split("time_list", ",")[1]).withColumn("count", split("time_list", ",")[2]).drop("time_list")
        # 到此将hdfs中旧的track数据更新完毕, 这里写回旧表，以供后续继续判断
        # df_ele_fence.write.mode(saveMode='overwrite').jdbc(url=self.PGSQL_URL1, table="analysis_etl_gd_ele_fence.atrack", properties=self.PGSQL_PROPERTIES)
        if df_ele_fence.count() > 0:
            df_ele_fence.write.csv(self.HDFS_ST_TRACK, header=True, mode='overwrite')

        return df_ele_fence

    def standardize_etl_data(self, df_track_source):
        """标准化清洗完的数据，并转为pgsql中track表的格式，供后续更新"""
        func_start_flag = udf(lambda x: UdfUtils().udf_start_end_flag(x, 0), StringType())
        func_end_flag = udf(lambda x: UdfUtils().udf_start_end_flag(x, 1), StringType())
        # tra_s [mobile_mac, netbar_wacode, "time_start,0", "time_end,1", count]
        df_track_source = df_track_source.withColumn("time_start", func_start_flag("time_start")).withColumn("time_end", func_end_flag("time_end"))
        # tra_t [mobile_mac, netbar_wacode, probe_time("time_start,0-time_end,1"), count]
        df_track_tmp = df_track_source.select(concat_ws('-', df_track_source.time_start, df_track_source.time_end).alias('probe_time'), 'mobile_mac', 'netbar_wacode')
        # tra_t [mobile_mac, netbar_wacode, probe_time("time,flag"), count]
        df_track_tmp = df_track_tmp.withColumn("probe_time", explode(split("probe_time", "-")))
        df_track_tmp = df_track_tmp.withColumn("flag", split("probe_time", ",")[1])
        # tra_t [mobile_mac, netbar_wacode, probe_time, flag, count]
        df_track_tmp = df_track_tmp.withColumn("probe_time", split("probe_time", ",")[0])
        # # tra_t [mobile_mac, netbar_wacode, probe_time, flag, count, probe_type]
        # df_track_tmp = df_track_tmp.withColumn("probe_time", df_track_tmp.probe_time.astype("long")).withColumn("probe_type", functions.lit("mobile_mac"))
        # tra_t [probe_data, netbar_wacode, probe_time, flag, count, probe_type, create_time]
        df_track_tmp = df_track_tmp.withColumnRenamed("mobile_mac", "probe_data")
        # tra_t [probe_data, netbar_wacode, probe_time, flag, count, probe_type, datasource_table_name]
        df_track_tmp = df_track_tmp.withColumn("datasource_table_name", functions.lit("gd_ele_fence"))
        # df_pla_info = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_GD_PLACE, properties=self.PGSQL_PROPERTIES)
        # # tra_t [probe_data, netbar_wacode, probe_time, flag, count, probe_type, create_time, datasource_table_name, longitude, latitude]
        # df_track_tmp = df_track_tmp.join(df_pla_info.select(["netbar_wacode", "longitude", "latitude"]), ["netbar_wacode"], "left")
        # # tra_t [probe_data, netbar_wacode, probe_time, flag, count, probe_type, datasource_table_name, longitude, latitude, device_type]
        # df_track_tmp = df_track_tmp.withColumn("device_type", functions.lit(1))  # .withColumn("probe_data", func_mac_o2h("probe_data"))
        # _schema = StructType([StructField("netbar_wacode")])# device_mac last_update_time count
        # # 添加device字段，如果有统计设备探测次数，就使用hdfs的数据，没有就gd_device中随机选一个
        # client = pyhdfs.HdfsClient(hosts=self.PYHDFS_HOST, user_name='hdfs')
        # # tra_o [probe_data, netbar_wacode, probe_time, count, probe_type, datasource_table_name, longitude, latitude, device_type, probe_device_mac, flag]
        # if client.exists(self.HDFS_ST_DEVICE_SUB):
        #     # max_dev [device_mac, update_time, netbar_wacode, count]
        #     df_max_dev = self.spark.read.csv(self.HDFS_ST_DEVICE, header=True)
        #     func_text2long = udf(UdfUtils().udf_text2long, LongType())
        #     # max_dev [device_mac, netbar_wacode, count]
        #     df_max_dev = df_max_dev.withColumn("count", func_text2long("count")).drop("update_time")
        #     # max_dev_tmp [netbar_wacode, count]    --找出最大count的netbar_wacode
        #     df_max_dev_tmp = df_max_dev.groupBy("netbar_wacode").max("count").withColumnRenamed("max(count)", "count")
        #     df_max_dev = df_max_dev_tmp.join(df_max_dev, ["netbar_wacode", "count"], "left")
        #     df_max_dev = df_max_dev.drop("count").drop("update_time").dropDuplicates(["netbar_wacode"])
        #     df_track_output = df_track_tmp.join(df_max_dev, ["netbar_wacode"], "left").withColumnRenamed("device_mac", "probe_device_mac")
        # else:
        #     df_dev = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_GD_DEVICE, properties=self.PGSQL_PROPERTIES)
        #     df_dev = df_dev.select("netbar_wacode", "device_mac").dropDuplicates(["netbar_wacode"])
        #     df_track_output = df_track_tmp.join(df_dev, ["netbar_wacode"], "left").withColumnRenamed("device_mac", "probe_device_mac")
        # # tra_o [probe_data, netbar_wacode, probe_time, count, probe_type, datasource_table_name, longitude, latitude, device_type, probe_device_mac, flag]
        # df_track_output = df_track_output.withColumn("device_type", df_track_output.device_type.astype("long"))

        func_null_to_zero = udf(UdfUtils().udf_null_to_zero, StringType())
        df_track_output = df_track_tmp
        df_track_output = df_track_output.withColumn("flag", func_null_to_zero("flag"))

        # tra_o_2 [netbar_wacode, probe_time, probe_data, probe_type, datasource_table_name, longitude, latitude, device_type, probe_device_mac]
        df_track_output_2_hdfs = df_track_output.drop("flag")
        # 至此将所有更新后的track的数据都转为track表中的格式

        # tra_o [netbar_wacode, probe_time, probe_data, probe_type, datasource_table_name, longitude, latitude, device_type, probe_device_mac, flag]
        # tra_o [netbar_wacode, probe_time, probe_data, datasource_table_name, flag]
        return df_track_output

    def update_track_talbe(self, df_track_output, file_path):
        """执行更新操作，将需要更新的数据列出，将新的数据追加到track表中"""
        # df_track_output [netbar_wacode, probe_time, probe_data, probe_type, datasource_table_name, longitude, latitude, device_type, probe_device_mac, flag]
        # tra_o [netbar_wacode, probe_time, probe_data, datasource_table_name, flag]
        # 旧track表的数据
        # 到时候df_track_old读取的数据（track表）维护在hdfs里，最后写回hdfs与pgsql
        # df_track_old[id,probe_time,probe_type,probe_data,probe_device_mac,netbar_wacode,longitude,latitude,device_type,create_time,datasource_id,datasource_table_name,base_person_id,flag]
        client = pyhdfs.HdfsClient(hosts=self.PYHDFS_HOST, user_name='hdfs')
        if client.exists(self.HDFS_ST_TRACK_INFO_SUB):
            df_track_old = self.spark.read.parquet(self.HDFS_ST_TRACK_INFO)
            df_track_old = df_track_old.select("probe_time", "probe_data", "netbar_wacode", "flag", "datasource_table_name")
        else:
            # df_track_old = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_TRACK, properties=self.PGSQL_PROPERTIES).limit(0).withColumn("flag", functions.lit(0)).withColumn("probe_data", functions.lit(""))
            # df_track_old = df_track_old.select("probe_time", "probe_data", "netbar_wacode", "flag", "datasource_table_name")
            schema = StructType([
                StructField("probe_time", StringType()), StructField("probe_data", StringType()), StructField("netbar_wacode", StringType()), StructField("flag", StringType()), StructField("datasource_table_name", StringType())
            ])
            df_track_old = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        # df_track_old = self.spark.read.csv(self.HDFS_ST_TRACK, head=True)
        df_track_old = df_track_old[df_track_old["datasource_table_name"] == "gd_ele_fence"]
        # df_track_old = df_track_old.withColumn("device_type", df_track_old.device_type.astype("long"))
        # # _df_track_old  [netbar_wacode,probe_time,probe_data,probe_type,datasource_table_name,longitude,latitude,device_type,probe_device_mac,flag]
        # _df_track_old = df_track_old.select("netbar_wacode", "probe_time", "probe_data", "probe_type", "datasource_table_name", "longitude", "latitude", "device_type", "probe_device_mac", "flag")  # , "last_update_time"

        # 当结构两个df结构没有完全相同，例如nullable，subtract不会求差
        # df_track_append [netbar_wacode,probe_time,probe_data,probe_type,datasource_table_name,longitude,latitude,device_type,probe_device_mac,flag]
        df_track_append = df_track_output.subtract(df_track_old)

        # df_track_append = df_track_append.withColumn("device_type", df_track_append.device_type.astype("long"))
        df_track_append = df_track_append.withColumn("create_time", functions.lit(int(time.time() * 1000)))
        # [probe_time, probe_data, netbar_wacode, flag, datasource_table_name, create_time, datasource_id]
        df_track_append = df_track_append.withColumn("datasource_id", functions.monotonically_increasing_id())
        df_track_append = df_track_append.withColumn("probe_time", df_track_append.probe_time.astype("long"))
        func_x_1000 = udf(UdfUtils().udf_x_1000, LongType())
        df_track_append = df_track_append.withColumn("probe_time", func_x_1000("probe_time"))

        # 由于安乡数据没有正确场所，现在先将数据里的device用于添加device_mac
        # df_ele_fence = self.spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/ele_test.csv', header=True)
        # df_ele_fence = self.spark.read.csv('hdfs://192.168.7.150:8020/test/cjh/csv/ap_test.csv', header=True)
        # df_ele_fence = self.spark.read.jdbc(url="jdbc:postgresql://192.168.1.99:6543/postgres", table="analysis_etl_gd_ele_fence.test_data_copy1", properties=self.PGSQL_PROPERTIES)

        df_ele_fence = self.spark.read.parquet(file_path)

        df_dev = df_ele_fence.withColumnRenamed("collection_equipment_mac", "device_mac").withColumnRenamed("local_mac", "device_mac").withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("router_mac", "device_mac").select("netbar_wacode", "device_mac").dropDuplicates(["netbar_wacode"])
        df_dev = df_dev.withColumn("netbar_wacode", df_dev["netbar_wacode"].astype("string")).withColumn("device_mac", df_dev["device_mac"].astype("long"))
        func_mac_o2h = udf(UdfUtils().udf_mac_o2h, StringType())
        df_dev = df_dev.withColumn("device_mac", func_mac_o2h("device_mac"))
        df_track_append = df_track_append.join(df_dev, ["netbar_wacode"], "left")



        # # 添加device字段，如果有统计设备探测次数，就使用hdfs的数据，没有就gd_device中随机选一个
        # client = pyhdfs.HdfsClient(hosts=self.PYHDFS_HOST, user_name='hdfs')
        # # tra_o [probe_data, netbar_wacode, probe_time, count, probe_type, datasource_table_name, longitude, latitude, device_type, probe_device_mac, flag]
        # if client.exists(self.HDFS_ST_DEVICE_SUB):
        #     # max_dev [device_mac, update_time, netbar_wacode, count]
        #     df_max_dev = self.spark.read.csv(self.HDFS_ST_DEVICE, header=True)
        #     func_text2long = udf(UdfUtils().udf_text2long, LongType())
        #     # max_dev [device_mac, netbar_wacode, count]
        #     df_max_dev = df_max_dev.withColumn("count", func_text2long("count")).drop("update_time")
        #     # max_dev_tmp [netbar_wacode, count]    --找出最大count的netbar_wacode
        #     df_max_dev_tmp = df_max_dev.groupBy("netbar_wacode").max("count").withColumnRenamed("max(count)", "count")
        #     df_max_dev = df_max_dev_tmp.join(df_max_dev, ["netbar_wacode", "count"], "left")
        #     df_max_dev = df_max_dev.drop("count").drop("update_time").dropDuplicates(["netbar_wacode"])
        #     df_track_append = df_track_append.join(df_max_dev, ["netbar_wacode"], "left")
        # else:
        #     df_dev = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_GD_DEVICE, properties=self.PGSQL_PROPERTIES)
        #     df_dev = df_dev.select("netbar_wacode", "device_mac").dropDuplicates(["netbar_wacode"])
        #     df_track_append = df_track_append.join(df_dev, ["netbar_wacode"], "left")
        df_dev = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_GD_DEVICE, properties=self.PGSQL_PROPERTIES).select("id", "device_mac")
        df_track_append = df_track_append.join(df_dev, ["device_mac"], "left")
        df_track_append = df_track_append.drop("device_mac").withColumnRenamed("id", "probe_device_id")

        # 此为有flag版，到时候插入轨迹表需要将flag字段去掉
        # df_track_append.write.mode(saveMode='append').jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.track_copy3", properties=self.PGSQL_PROPERTIES)
        df_track_append.drop("flag").drop("probe_data").write.mode(saveMode='append').jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_TRACK, properties=self.PGSQL_PROPERTIES)
        print("append : " + str(df_track_append.count()))
        if df_track_append.count() > 0:
            df_track_append.write.parquet(self.HDFS_ST_TRACK_INFO, mode="append")
        # df_track_append [netbar_wacode, probe_time, probe_data, flag, datasource_table_name, create_time, datasource_id, probe_device_id]
        return df_track_append

    def update_attr_record(self, df_track_append):
        # df_track_append [netbar_wacode, probe_time, probe_data, flag, datasource_table_name, create_time, datasource_id, probe_device_id]
        # df_track [id, probe_time, netbar_wacode, create_time, datasource_id, datasource_table_name, base_person_id, probe_device_id]
        df_track = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_TRACK, properties=self.PGSQL_PROPERTIES)
        df_track = df_track[df_track["datasource_table_name"] == "gd_ele_fence"]
        # netbar_wacode加了就有问题，probe_time加了就有问题 , "create_time", "datasource_id"没问题， "probe_time", "datasource_id"显示create_time被忽视
        df_track_append = df_track_append.join(df_track.drop("base_person_id"), ["create_time", "datasource_id"], "left")  # "probe_time", "netbar_wacode", "create_time", "datasource_id"   , "datasource_table_name", "probe_device_id"
        df_track_append = df_track_append.withColumnRenamed("id", "track_id")
        df_attr = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_ATTR, properties=self.PGSQL_PROPERTIES)
        df_attr = df_attr[df_attr["attr_type_id"] == 5].select("id", "attr_value").withColumnRenamed("attr_value", "probe_data")
        df_track_append = df_track_append.join(df_attr, ["probe_data"], "left")
        df_track_append = df_track_append.withColumnRenamed("id", "attr_id").withColumn("create_time", functions.lit(int(time.time() * 1000))).select("track_id", "attr_id", "create_time")
        df_track_append.write.mode(saveMode='append').jdbc(url=self.PGSQL_URL_analysis, table=self.PGSQL_TABLE_ATTR_RECORD, properties=self.PGSQL_PROPERTIES)


class UdfUtils(object):
    def udf_rounded_up(self, time_on, span):
        """按指定间隔舍入数据"""
        tmp = span / 2
        if time_on % span > tmp:
            return int(int(time_on / span) * span + span)
        elif time_on % span <= tmp:
            return int(int(time_on / span) * span)

    def udf_text2long(self, data):
        """某个字段的数据转为int类型，原字段类型一般为不能直接转类型的text"""
        return int(data)

    def udf_x_1000(self, data):
        return data * 1000

    def udf_2_list_string(self, data):
        """某个字段的数据加上[]"""
        return '[' + str(data) + ']'

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

    def udf_track_union(self, time_list, timestack_span):
        time_list = str(time_list)[3: -3].split(']\', \'[')
        time_list = [x.split(",") for x in time_list]
        time_list = [[int(i) for i in x] for x in time_list]
        time_list = sorted(time_list)
        # return time_list
        for i in range(len(time_list) - 1):
            # if time_list[i + 1][0] - time_list[i][1] > timestack_span:
            #     pass
            # if time_list[i + 1][0] - time_list[i][1] <= timestack_span:
            #     time_list[i + 1][0] = time_list[i][0]
            #     time_list[i][1] = time_list[i + 1][1]
            #     sum = time_list[i][2] + time_list[i + 1][2]
            #     time_list[i][2] = sum
            #     time_list[i + 1][2] = sum

            if time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1]:     # 新被旧包含
                time_list[i + 1][0] = time_list[i][0]
                time_list[i + 1][1] = time_list[i][1]
                sum = time_list[i][2] + time_list[i + 1][2]
                time_list[i][2] = sum
                time_list[i + 1][2] = sum
            elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1]:   # 旧被新包含
                time_list[i][0] = time_list[i + 1][0]
                time_list[i][1] = time_list[i + 1][1]
                sum = time_list[i][2] + time_list[i + 1][2]
                time_list[i][2] = sum
                time_list[i + 1][2] = sum
            elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1] and time_list[i][0] - time_list[i + 1][1] <= timestack_span:   # 新在旧左边
                time_list[i][0] = time_list[i + 1][0]
                time_list[i + 1][1] = time_list[i][1]
                sum = time_list[i][2] + time_list[i + 1][2]
                time_list[i][2] = sum
                time_list[i + 1][2] = sum
            elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1] and time_list[i][0] - time_list[i + 1][1] > timestack_span:
                pass
            elif time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1] and time_list[i + 1][0] - time_list[i][1] <= timestack_span:   # 新在旧右边
                time_list[i + 1][0] = time_list[i][0]
                time_list[i][1] = time_list[i + 1][1]
                sum = time_list[i][2] + time_list[i + 1][2]
                time_list[i][2] = sum
                time_list[i + 1][2] = sum
            elif time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1] and time_list[i + 1][0] - time_list[i][1] > timestack_span:
                pass
        time_list_output = []
        for element in time_list:
            if (element not in time_list_output):
                time_list_output.append(element)
        return str(time_list_output)[2:-2]
        # return time_list

    def udf_start_end_flag(self, data, flag):
        return str(data) + "," + str(flag)

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


if __name__ == '__main__':
    conf = SparkConf().setAppName('spark_clean')
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.core", "2")
    conf.set("spark.scheduler.mode", "FIFO")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # if len(sys.argv) > 1:
    #     conf_param = json.loads(sys.argv[1])
    #     AnalysisEleFence(spark).etl_real_time(conf_param['file_path'], conf_param['precision_span'],
    #                                           conf_param['timestack_span'])

    AnalysisEleFence(spark).etl_real_time('hdfs://192.168.7.150:8020/co_ele_fence/newest', 60, 600)

    # df = spark.read.parquet('hdfs://192.168.7.150:8020/co_ele_fence/newest')
    # df.groupBy("mobile_mac").count().show(1000)

    spark.stop()
