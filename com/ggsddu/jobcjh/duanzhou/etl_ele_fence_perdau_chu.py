# -*- encoding: utf-8 -*-
"""
@File       :   etl_ele_fence_perdau_chu.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/10/13 17:50
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
# -*- encoding: utf-8 -*-
"""
@File       :   ele_fence_analysis.py
@Contact    :   ggsddu.com
@Modify Time:   2020/7/24 10:24
@Author     :   cjh
@Version    :   1.0
@Desciption :   对电子围栏数据进行各种处理
"""
import time
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf, split, explode, concat_ws, isnan, isnull
from pyspark.sql import functions


class AnalysisEleFence(object):
    def __init__(self, spark):
        self.spark = spark
        self.precision_span = 60
        self.timestack_span = 600

    def etl(self, data):
        """对所有的mobile_mac进行清洗，并入库"""
        # url = 'hdfs://192.168.7.150:8020/data_sync_test/anxiang/co_ele_fence/1591207203'
        url = 'hdfs://192.168.7.150:8020/data_etl/duanzhou' + str(data)
        df_ele_fence = self.spark.read.parquet(url)
        df_ele_fence = df_ele_fence.withColumnRenamed("ap_mac", "mobile_mac").withColumnRenamed("place_code", "netbar_wacode").withColumnRenamed("local_mac", "collection_equipment_mac").withColumnRenamed("router_mac", "collection_equipment_mac")
        df_ele_fence = df_ele_fence.select('time_on', 'mobile_mac', 'netbar_wacode', 'collection_equipment_mac')
        # 将time_on精度调整为一分钟
        func_rounded_up = udf(lambda x: UdfUtils().udf_rounded_up(x, self.precision_span), LongType())
        df_ele_fence = df_ele_fence.withColumn("time_on", func_rounded_up("time_on"))
        # 找到每个mobile_mac在某个time_on被探测到最大次数时所在的场所，并保留此探测次数
        df_ele_fence = df_ele_fence.groupBy(["mobile_mac", "netbar_wacode", "time_on"]).agg(functions.count("time_on")).withColumnRenamed("count(time_on)", "count")
        df_ele_fence = df_ele_fence.groupBy(["mobile_mac", "netbar_wacode", "time_on"]).max("count").withColumnRenamed("max(count)", "max_count") # .sort("time_on") # 这里sort没用
        # [mobile_mac, netbar_wacode, time_on, max_count]
        df_ele_fence = df_ele_fence.withColumn("max_count", df_ele_fence.max_count.astype("string"))
        df_ele_fence = df_ele_fence.withColumn("time_on", df_ele_fence.time_on.astype("string"))

        # 将time_on和max_count合并
        df_ele_fence = df_ele_fence.select(concat_ws(',', df_ele_fence.time_on, df_ele_fence.max_count).alias('time_on'), "mobile_mac", "netbar_wacode")
        func_2_list_string = udf(UdfUtils().udf_2_list_string, StringType())
        df_ele_fence = df_ele_fence.withColumn("time_on", func_2_list_string("time_on"))

        # 将[time_on, max_count]按mobile_mac和netbar_wacode分组，并合成list
        df_ele_fence = df_ele_fence.groupBy(["mobile_mac", "netbar_wacode"]).agg(functions.collect_list('time_on').alias('time_list')) # .drop("max(count)")

        # 删除没用的数据并按一定规则生成轨迹（开始时间+结束时间）
        func_etl = udf(lambda x: UdfUtils().udf_time_etl(x, self.timestack_span), StringType())
        df_ele_fence = df_ele_fence.withColumn("time_list", func_etl("time_list"))

        # explode成多个轨迹，并将轨迹拆分成time_start、time_end、count
        df_ele_fence = df_ele_fence.withColumn("time_list", explode(split("time_list", "\\]', '\\[")))
        df_ele_fence = df_ele_fence.withColumn("time_start", split("time_list", ",")[0]).withColumn("time_end", split("time_list", ",")[1]).withColumn("count", split("time_list", ",")[2]).drop("time_list")
        df_ele_fence = df_ele_fence.withColumn("time_start", df_ele_fence.time_start.astype("long"))
        func_text2long = udf(UdfUtils().udf_text2long, LongType())
        df_ele_fence = df_ele_fence.withColumn("time_end", func_text2long("time_end"))
        df_ele_fence = df_ele_fence.withColumn("count", func_text2long("count"))
        df_ele_fence = df_ele_fence[df_ele_fence["mobile_mac"] != 0]
        func_mac_o2h = udf(UdfUtils().udf_mac_o2h, StringType())
        df_ele_fence = df_ele_fence.withColumn("mobile_mac", func_mac_o2h("mobile_mac"))
        df_ele_fence = df_ele_fence.repartition(1)
        df_ele_fence.write.parquet('hdfs://192.168.7.150:8020/data_etl/' + str(data), mode='overwrite')


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

    def udf_2_list_string(self, data):
        """某个字段的数据加上[]"""
        return '[' + str(data) + ']'

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
    conf = SparkConf().setAppName('cjh')
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "8g")
    conf.set("spark.num.executors", "3")
    conf.set("spark.executor.core", "3")
    conf.set("spark.default.parallelism", "20")
    conf.set("spark.memory.fraction", "0.75")
    conf.set("spark.memory.storageFraction", "0.75")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # date = ['0806', '0807', '0808', '0809', '0810', '0811', '0812', '0813', '0814', '0815', '0816', '0817', '0818', '0819', '0820', '0821', '0822', '0823', '0824', '0825', '0826', '0827']
    date = ['0709', '0710', '0711', '0712', '0713', '0714', '0715', '0716', '0717', '0718', '0719', '0720', '0721', '0722', '0723', '0724', '0725', '0726', '0727', '0728', '0729', '0730', '0731', '0801', '0802', '0803', '0804', '0805', '0806', '0807', '0808', '0809', '0810', '0811', '0812', '0813', '0814', '0815', '0816', '0817', '0818', '0819', '0820', '0821', '0822', '0823', '0824', '0825', '0826']
    for i in range(len(date)):
        # AnalysisEleFence(spark).etl(date[i], 60, 600)
        url1 = 'hdfs://192.168.7.150:8020/data_etl/duanzhou/2020'
        df = spark.read.parquet(url1 + date[i]).repartition(1)
        url2 = 'hdfs://192.168.7.150:8020/data_etl/2020'
        df.write.parquet(url2 + date[i], mode='overwrite')

    # AnalysisEleFence(spark).etl(source_path, )
    # spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/par/*').show()
    # df = spark.read.parquet('hdfs://192.168.7.150:8020/test/cjh/par/ele1591207203').repartition(1)
    # df.write.parquet('hdfs://192.168.7.150:8020/test/cjh/par/test', mode='overwrite')
    spark.stop()
