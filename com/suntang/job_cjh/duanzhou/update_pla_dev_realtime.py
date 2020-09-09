# -*- encoding: utf-8 -*-
"""
@File       :   update_pla_dev_realtime.py    
@Contact    :   suntang.com
@Modify Time:   2020/9/3 18:02
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
# -*- encoding: utf-8 -*-
"""
@File       :   st_update_act_pla_dev.py    
@Contact    :   suntang.com
@Modify Time:   2020/8/27 16:31
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import uuid
import json
import copy
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions
from rocketmq.client import Producer, Message

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
        self.PGSQL_URL1 = "jdbc:postgresql://192.168.1.99:6543/postgres"
        self.PGSQL_URL_analysis = "jdbc:postgresql://192.168.1.99:6543/police_analysis_db"
        self.PGSQL_PROPERTIES = {'user': 'postgres', 'password': 'postgres'}
        self.HDFS_ST_PLACE = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place'
        self.HDFS_ST_DEVICE = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device'
        self.HDFS_ST_PLACE_TMP = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place_tmp'
        self.HDFS_ST_DEVICE_TMP = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device_tmp'
        self.HDFS_ST_PLACE_INFO = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_place_info'
        self.HDFS_ST_DEVICE_INFO = 'hdfs://192.168.7.150:8020/test/cjh/dev_pla/st_device_info'

    def device_place_info_2hdfs_real_time(self, date):
        """更新场所与设备的update_time，并将更新的数据放到hdfs中，但不会更新设备探测次数，需要调用count_dev_probe_2hdfs进行更新"""
        update_job = UpdateTimeEleFence(self.spark)
        # df_pla_old[netbar_wacode, longitude, latitude, update_time],每天的更新以及场所设备的更新需要在这一天实时更新之后,这样才不会动场所设备的内容
        # df_dev_old[device_mac,update_time,netbar_wacode,count]
        df_pla_old = self.spark.read.csv(self.HDFS_ST_PLACE, header=True).select(["netbar_wacode", "longitude", "latitude", "update_time"])
        df_dev_old = self.spark.read.csv(self.HDFS_ST_DEVICE, header=True)
        df_ef = self.spark.read.parquet(date)
        df_ef = update_job.prepare_ele_fence(df_ef)
        # _df_pla_old[netbar_wacode, update_time]
        _df_pla_old = update_job.prepare_pla(df_pla_old)
        # df_dev_old[device_mac,update_time,netbar_wacode,count]
        df_dev_old = update_job.prepare_dev(df_dev_old)
        update_job.update_time_pla(df_ef, _df_pla_old, df_pla_old)
        update_job.update_time_dev(df_ef, df_dev_old)

    def prepare_ele_fence(self, df_ef):
        """实时的数据,并进行数据预处理"""
        df_ef = df_ef.select(["netbar_wacode", "collection_equipment_mac", "time_on"]).withColumnRenamed(
            "collection_equipment_mac", "device_mac")
        func_x1000 = udf(UdfUtils().udf_x1000, LongType())
        df_ef = df_ef.withColumn("update_time", func_x1000("time_on")).drop("time_on")
        func_null_to_zero = udf(UdfUtils().udf_null_to_zero, StringType())
        df_ef = df_ef.withColumn("device_mac", func_null_to_zero("device_mac")).withColumn("netbar_wacode",
                                                                                           func_null_to_zero(
                                                                                               "netbar_wacode"))
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
        """场所表updatetime更新，具体输出dataframe格式为[id, updatetime]"""
        # df_pla_old[netbar_wacode, longitude, latitude, update_time]
        # _df_pla_old[netbar_wacode, update_time]
        # df_ef [netbar_wacode, device_mac, update_time]
        # _df_pla_new[netbar_wacode, update_time_new]
        _df_pla_new = df_ef.select(["netbar_wacode", "update_time"]).groupBy("netbar_wacode").max(
            "update_time").withColumnRenamed("max(update_time)", "update_time_new")
        _df_pla_old = _df_pla_old.withColumn("update_time", _df_pla_old.update_time.astype("long"))
        # df_pla_new[netbar_wacode, update_time, update_time_new]
        df_pla_new = _df_pla_old.join(_df_pla_new, ["netbar_wacode"], "outer")
        func_select_field = udf(lambda x, y: UdfUtils().udf_select_filed(x, y), StringType())
        # df_pla_new[netbar_wacode, update_time]
        df_pla_new = df_pla_new.withColumn("update_time", func_select_field("update_time", "update_time_new")).drop(
            "update_time_new")
        # df_pla_new[netbar_wacode, update_time, longitude, latitude]
        df_pla_new = df_pla_new.join(df_pla_old.drop("update_time"), ["netbar_wacode"], "left")
        # 不该用，得最后一整天在计算count，不然会影响数据清洗
        # df_pla_new.write.csv(self.HDFS_ST_PLACE, header=True, mode='overwrite')
        df_pla_old = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_place",
                                          properties=self.PGSQL_PROPERTIES)
        func_delete_space = udf(lambda x: UdfUtils().udf_delete_string_elemt(x, " "), StringType())
        # df_pla_old[netbar_wacode, id]
        df_pla_old = df_pla_old.withColumn("netbar_wacode", func_delete_space("netbar_wacode")).select(
            ["netbar_wacode", "id"])

        df_pla_sub = df_pla_new.select("netbar_wacode").subtract(df_pla_old.select("netbar_wacode"))
        df_pla_sub = df_pla_sub.join(df_pla_new, ["netbar_wacode"], "left")
        df_pla_sub = df_pla_sub.withColumn("update_time", df_pla_sub.update_time.astype("long"))

        # df_pla_old有的才更新
        # df_pla_new[id, update_time]
        df_pla_new = df_pla_old.join(df_pla_new.select(["netbar_wacode", "update_time"]), ["netbar_wacode"],
                                     "left").drop("netbar_wacode")
        df_pla_new.write.csv(self.HDFS_ST_PLACE_INFO, header=True, mode='overwrite')

        df_pla_new = df_pla_new.withColumn("update_time", df_pla_new.update_time.astype("long"))
        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_pla_new = df_pla_new.withColumn("update_time", func_null_to_0("update_time"))
        # 去除掉没有更新的场所，即update_time为空
        df_pla_new = df_pla_new[df_pla_new["update_time"] > 0]
        df_pla_new.show()

    def update_time_dev(self, df_ef, df_dev_old):
        """设备表updatetime更新，具体输出dataframe格式为[id, updatetime]"""
        # df_dev_old [device_mac,update_time,netbar_wacode,count]
        # df_ef [netbar_wacode, device_mac, update_time]
        # _df_dev_new [device_mac, update_time_new]
        _df_dev_new = df_ef.select(["device_mac", "update_time"]).groupBy("device_mac").max("update_time").withColumnRenamed("max(update_time)", "update_time_new")
        # _df_dev_old [device_mac,update_time,netbar_wacode,count]
        _df_dev_old = df_dev_old.withColumn("update_time", df_dev_old.update_time.astype("long"))
        # df_dev_new [device_mac,netbar_wacode,count,update_time,update_time_new]

        # _df_ef_dev_count[netbar_wacode, device_mac, count]
        _df_ef_dev_count = df_ef.groupBy(["netbar_wacode", "device_mac"]).count()
        # _df_ef_dev_true[device_mac, count]
        _df_ef_dev_true = _df_ef_dev_count.groupBy("device_mac").max("count").withColumnRenamed("max(count)", "count")  # .drop("max(count)")
        _df_ef_dev_true = _df_ef_dev_true[_df_ef_dev_true["count"] >= 3]
        # _df_ef_dev_true [netbar_wacode, device_mac]
        _df_ef_dev_true = _df_ef_dev_true.join(_df_ef_dev_count, ["device_mac", "count"], "left").drop("count")
        _df_ef_dev_true = _df_ef_dev_true.subtract(df_ef.select("netbar_wacode", "device_mac").dropDuplicates())
        _df_ef_dev_true = _df_ef_dev_true.join(_df_dev_new, ["device_mac"], "left")
        _df_ef_dev_true = _df_ef_dev_true.withColumnRenamed("update_time_new", "update_time")
        df_dev_sub = _df_ef_dev_true.withColumn("update_time", _df_ef_dev_true.update_time.astype("long"))

        df_dev_new = _df_dev_old.join(_df_dev_new, ["device_mac"], "outer")
        func_select_field = udf(lambda x, y: UdfUtils().udf_select_filed(x, y), StringType())
        # df_dev_new [device_mac,netbar_wacode,count,update_time]
        df_dev_new = df_dev_new.withColumn("update_time", func_select_field("update_time", "update_time_new")).drop("update_time_new")
        # _df_dev_new.write.csv(self.HDFS_ST_DEVICE, header=True, mode='overwrite')
        df_dev_old = self.spark.read.jdbc(url=self.PGSQL_URL_analysis, table="zhaoqing_duanzhou_db.gd_device", properties=self.PGSQL_PROPERTIES)
        # df_dev_old [device_mac, id]
        df_dev_old = df_dev_old.select(["device_mac", "id"])
        # df_dev_new [id, update_time]
        df_dev_new = df_dev_old.join(df_dev_new.select(["device_mac", "update_time"]), ["device_mac"], "left").drop("device_mac")
        df_dev_new.write.csv(self.HDFS_ST_DEVICE_INFO, header=True, mode='overwrite')
        df_dev_new = df_dev_new.withColumn("update_time", df_dev_new.update_time.astype("long"))

        func_null_to_0 = udf(UdfUtils().udf_null_to_0, LongType())
        df_dev_new = df_dev_new.withColumn("update_time", func_null_to_0("update_time"))
        df_dev_new = df_dev_new[df_dev_new["update_time"] > 0]
        df_dev_new.show()

        # data_list = df_dev_new.toJSON().collect()
        # print(data_list)
        # producer = Producer('policeTracePythonProducer')
        # producer.set_namesrv_addr('192.168.9.214:9876')
        # producer.start()
        # while data_list:
        #     msg_body = copy.deepcopy(MSG_TEMPLATE)
        #     msg_body['param']['conn']['table'] = "gd_device"
        #     msg = Message("SparkPlan")
        #     msg.set_tags(str(uuid.uuid1()))
        #     if len(data_list) >= 100:
        #         send_data_list = data_list[:100]
        #         data_list = data_list[100:]
        #     else:
        #         send_data_list, data_list = data_list, None
        #     msg_body['data'] = f"[{','.join(send_data_list)}]"
        #     msg.set_body(json.dumps(msg_body))
        #     producer.send_sync(msg)
        # producer.shutdown()


class UdfUtils(object):
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

    def udf_null_to_0(self, data):
        if not data:
            data = 0
        return data


if __name__ == '__main__':
    conf = SparkConf().setAppName('device')
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.driver.memory", "2")
    conf.set("spark.executor.core", "3")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    UpdateTimeEleFence(spark).device_place_info_2hdfs_real_time('')
    spark.stop()
