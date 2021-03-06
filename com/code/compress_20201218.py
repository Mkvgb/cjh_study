# -*- encoding: utf-8 -*-
"""
@File       :   transfer.py
@Contact    :   ggsddu.com
@Modify Time:   2020/10/13 17:55
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import sys
import json

import pyhdfs
from pyspark import SparkConf
from pyspark.sql.types import StringType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, split, explode, concat_ws
from pyspark.sql import functions
import time

# 'probe_time', 'probe_mac', 'place_code', 'collect_mac'
DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '5435'
DST_DB_USER = "postgres"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "zhaoqing_duanzhou_db"
DST_DB_ATTR_TABLE = "attr"
DST_DB_ATTR_RECORD_TABLE = "attr_record"
DST_DB_TRACK_TABLE = "track"

RABBIT_MQ_PREFIX = 'crash'
RABBIT_MQ_HOST = '192.168.1.99'
RABBIT_MQ_PORT = '5672'
RABBIT_MQ_USER = 'admin'
RABBIT_MQ_PASSWORD = 'admin'
RABBIT_MQ_VIRTUAL_HOST = '/'
RABBIT_MQ_EXCHANGE = f'{RABBIT_MQ_PREFIX}_direct_exchange'

RABBIT_MQ_PERSON_TRACK_QUEUE = f'{RABBIT_MQ_PREFIX}_person_trace_queue'
RABBIT_MQ_PERSON_TRACK_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_person_trace_router'

RABBIT_MQ_WARN_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_sms_router'
RABBIT_MQ_WARN_ROUTE_QUEUE = f'{RABBIT_MQ_PREFIX}_sms_queue'

RABBIT_MQ_TRACK_QUEUE = f'{RABBIT_MQ_PREFIX}_trace_queue'
RABBIT_MQ_TRACK_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_trace_router'

RABBIT_MQ_DATA_CACHE_QUEUE = f'{RABBIT_MQ_PREFIX}_cache_queue'
RABBIT_MQ_DATA_CACHE_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_cache_router'

RABBIT_MQ_SEARCH_QUEUE = f'{RABBIT_MQ_PREFIX}_search_queue'
RABBIT_MQ_SEARCH_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_search_router'

RABBIT_AMQP_URL = f'amqp://{RABBIT_MQ_USER}:{RABBIT_MQ_PASSWORD}@{RABBIT_MQ_HOST}:{RABBIT_MQ_PORT}/'
if not RABBIT_MQ_VIRTUAL_HOST == '/':
    RABBIT_AMQP_URL += RABBIT_MQ_VIRTUAL_HOST

SPARK_SEARCH_FIELDS_CHANGE = {
    "time_start": "time_on",
    "time_end": "time_off",
    "mobile_mac": "apMac",
    "netbar_wacode": "placeCode"
}

# spark初始化配置
SPARK_EXECUTOR_MEMORY_CONF = "spark.executor.memory"  # "3g"
SPARK_DRIVER_MEMORY_CONF = "spark.driver.memory"  # "3g"
SPARK_NUM_EXECUTORS_CONF = "spark.num.executors"  # "3"
SPARK_EXECUTOR_CORE_CONF = "spark.executor.core"  # "3"
SPARK_SCHEDULER_MODE_CONF = "spark.scheduler.mode"  # "FIFO"

# HDFS_DUANZHOU_FENCE_COMPRESS_URL = '/duanzhou/probe_type_compress/*'
# HDFS_DUANZHOU_HTTP_COMPRESS_URL = '/duanzhou/audit_type_compress/*'
HDFS_DUANZHOU_FENCE_COMPRESS_URL = '/weihai/src/probe_type/'
HDFS_DUANZHOU_HTTP_COMPRESS_URL = '/weihai/src/audit_type/'
HDFS_DUANZHOU_WIFI_COMPRESS_URL = '/weihai/src/wifi_type/'



DEVICE_UPDATER_PROBE_FIELDS_CHANGE = {"start_time": "time_on"}
DEVICE_UPDATER_WIFI_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_AUDIT_FIELDS_CHANGE = {"collect_time": "time_on"}
DEVICE_UPDATER_IM_FIELDS_CHANGE = {"collect_time": "time_on"}

REDIS_HOST = '192.168.1.99'
REDIS_DB = 4
REDIS_PORT = '6379'
REDIS_PASSWORD = 'ggsddu@police'

HDFS_HOSTS = [
    '192.168.7.150:9870',
    '192.168.7.151:9870',
    '192.168.7.152:9870',
]
HDFS_HOST = '192.168.7.150'

PRECISION_SPAN = 60000
TIMESTACK_SPAN = 600000     # 单位为毫秒

HDFS_AUDIT_COMPRESS_PATH = '/weihai/compress/audit_type'
HDFS_IM_COMPRESS_PATH = '/weihai/compress/im_type'

class CompressAndMergeData(object):
    def __init__(self, spark):
        self.spark = spark
        self.HDFS_CLIENT = pyhdfs.HdfsClient(hosts=HDFS_HOST+':9870', user_name='hdfs')
        self.HDFS_AUDIT = HDFS_AUDIT_COMPRESS_PATH
        self.HDFS_IM = HDFS_IM_COMPRESS_PATH
        self.probe_fields_change = [['start_time', 'probe_time']]
        self.wifi_fields_change = [['collect_time', 'probe_time'], ['wifi_mac', 'probe_mac']]

    def compress_data_src(self, source_path, target_path, filename, tablename):
        """对所有的probe_mac进行清洗，并入库"""
        source_path, target_path = self._format_path_args(source_path, target_path)
        if source_path == target_path:
            return
        if not self.HDFS_CLIENT.exists(source_path + filename):     # 源数据不存在，直接跳出
            return
        spark_src_df = self.spark.read.parquet(source_path + filename)
        # 压缩审计与即时通讯数据
        if tablename == 'audit_type' or tablename == 'im_type':
            self._compress_audit_im_data(spark_src_df, tablename, source_path)
            return
        # 压缩探针与wifi数据
        if tablename == 'probe_type' or tablename == 'wifi_type':
            self._compress_probe_wifi_data(spark_src_df, target_path, tablename, filename)

        # 源数据按时间分天
        combine_path = source_path.replace('src', 'combine')
        self._merge_src_parquet(source_path, combine_path, filename, tablename)
        return True

    def _audit_union(self, spark_df, path):
        if not self.HDFS_CLIENT.exists(path):
            spark_df.write.parquet(path + '/' + str(int(time.time())))
            return
        file_path_list = self.HDFS_CLIENT.listdir(path)
        if file_path_list:
            spark_df_old = self.spark.read.parquet(path + '/*')
            spark_df = spark_df.union(spark_df_old)
        spark_df = spark_df.dropDuplicates()
        spark_df.write.parquet(path + '/' + str(int(time.time())))
        for i in range(len(file_path_list)):
            self.HDFS_CLIENT.delete(path + '/' + file_path_list[i], recursive=True)
        return True

    def _compress_probe_wifi_data(self, spark_df, target_path, tablename, filename):
        """压缩probe与wifi的数据"""
        spark_df_new = self._prepare_probe_wifi_data(spark_df, tablename)

        spark_df_new = self._get_main_place_by_time(spark_df_new)

        spark_df_new = self._get_compress_df(spark_df_new)

        spark_df_output = self._format_output_data(spark_df_new)
        # 时间不分开存储的版本
        # self._compress_output_no_merge(spark_df_output, target_path, filename)
        # 时间分开存储的版本
        self._compress_output_merge(spark_df_output, target_path, flag='compress', tablename=tablename)

    def _prepare_probe_wifi_data(self, spark_df, tablename):
        """对数据进行预处理，进行字段修改与字段选取"""
        if tablename == 'probe_type':
            for i in range(len(self.probe_fields_change)):
                spark_df = spark_df.withColumnRenamed(self.probe_fields_change[i][0], self.probe_fields_change[i][1])
        elif tablename == 'wifi_type':
            for i in range(len(self.wifi_fields_change)):
                spark_df = spark_df.withColumnRenamed(self.wifi_fields_change[i][0], self.wifi_fields_change[i][1])
        else:
            print("tablename is not probe_type or wifi_type!")
            return 0
        spark_df_new = spark_df.select('probe_time', 'probe_mac', 'place_code', 'collect_mac')
        return spark_df_new

    def _get_main_place_by_time(self, spark_df):
        """筛选出mac在某个时间段，出现的主要场所的一一对应关系"""
        func_rounded_up = udf(lambda x: x and SparkUdfSet().udf_rounded_up(x, PRECISION_SPAN) or 0, LongType())
        spark_df = spark_df.withColumn("probe_time", func_rounded_up("probe_time"))
        # 找到每个probe_mac在某个probe_time被探测到最大次数时所在的场所，并保留此探测次数
        spark_df = spark_df.groupBy(["probe_mac", "place_code", "probe_time"]).agg(functions.count("probe_time"))
        spark_df = spark_df.withColumnRenamed("count(probe_time)", "count")
        spark_df = spark_df.groupBy(["probe_mac", "place_code", "probe_time"]).max("count")
        spark_df = spark_df.withColumnRenamed("max(count)", "max_count")
        # [probe_mac, place_code, probe_time, max_count]
        spark_df = spark_df.withColumn("max_count", spark_df.max_count.astype("string")).withColumn("probe_time", spark_df.probe_time.astype("string"))
        return spark_df

    def _get_compress_df(self, spark_df_new):
        """按一定规则将所有轨迹进行压缩"""
        # 将probe_time和max_count合并
        spark_df_new = spark_df_new.select(concat_ws(',', spark_df_new.probe_time, spark_df_new.max_count).alias('probe_time'), "probe_mac", "place_code")
        func_2_list_string = udf(SparkUdfSet().udf_2_list_string, StringType())
        spark_df_new = spark_df_new.withColumn("probe_time", func_2_list_string("probe_time"))

        # 将[probe_time, max_count]按probe_mac和place_code分组，并合成list
        spark_df_new = spark_df_new.groupBy(["probe_mac", "place_code"]).agg(functions.collect_list('probe_time').alias('time_list'))  # .drop("max(count)")
        # 删除没用的数据并按一定规则生成轨迹（开始时间+结束时间）
        # func_etl = udf(lambda x: udf_time_etl(x, TIMESTACK_SPAN), StringType())
        func_etl = udf(lambda x: SparkUdfSet().udf_time_etl(x, TIMESTACK_SPAN), StringType())
        spark_df_new = spark_df_new.withColumn("time_list", func_etl("time_list"))
        # explode成多个轨迹，并将轨迹拆分成time_start、time_end、count
        spark_df_new = spark_df_new.withColumn("time_list", explode(split("time_list", "\\]', '\\[")))
        spark_df_new = spark_df_new.withColumn("time_start", split("time_list", ",")[0]).withColumn(
            "time_end", split("time_list", ",")[1]).withColumn(
            "count", split("time_list", ",")[2]).drop("time_list")
        return spark_df_new

    def _compress_audit_im_data(self, spark_src_df, tablename, source_path):
        """合并audit与im的数据, flag是源路径与目标路径是否一样的标志"""
        combine_path = source_path.replace('src', 'combine')
        if tablename == 'audit_type':
            self._audit_union(spark_src_df, combine_path)
        if tablename == 'im_type':
            self._audit_union(spark_src_df, combine_path)

    def _format_path_args(self, source_path, target_path):
        """对输入输出路径进行格式化"""
        if source_path[len(source_path) - 1] != '/':
            source_path = source_path + '/'
        if target_path[len(target_path) - 1] != '/':
            target_path = target_path + '/'
        return source_path, target_path

    def _format_output_data(self, spark_df):
        """对压缩完的数据进行格式标准化处理，进行字段转换与去脏"""
        spark_df = spark_df.withColumn("time_start", spark_df.time_start.astype("long"))
        func_text2long = udf(SparkUdfSet().udf_text2long, LongType())
        spark_df = spark_df.withColumn("time_end", func_text2long("time_end"))
        spark_df = spark_df.withColumn("count", func_text2long("count"))
        spark_df = spark_df.withColumn("probe_mac", spark_df["probe_mac"].astype("string"))
        spark_df = spark_df[spark_df["probe_mac"] != '0']
        spark_df = spark_df.withColumnRenamed('probe_mac', 'mac')
        spark_df = spark_df.withColumnRenamed('time_start', 'time_on').withColumnRenamed('time_end', 'time_off')
        return spark_df

    def _compress_output_no_merge(self, spark_df, target_path, filename):
        spark_df = spark_df.repartition(1)
        spark_df.write.parquet(target_path + filename, mode='overwrite')

    def _compress_output_merge(self, df_ele_fence, target_path, flag, tablename):
        if tablename == 'audit_type' or tablename == 'im_type':
            return
        time_field = ''
        if flag == 'compress':
            time_field = 'time_on'
        elif tablename == 'wifi_type':
            time_field = 'collect_time'
        elif tablename == 'probe_type':
            time_field = 'start_time'
        time_range_list = self._df_time_block_list(df_ele_fence, time_field)
        print(time_range_list)
        self._hdfs_merge_by_time(time_range_list, target_path, df_ele_fence, time_field)
        return True

    def _merge_src_parquet(self, input_path, output_path, filename, tablename):
        df_ele_fence = self.spark.read.parquet(input_path + filename)
        time_field = ''
        if tablename == 'probe_type':
            time_field = 'start_time'
        if (tablename == 'wifi_type') or tablename == 'audit_type' or tablename == 'im_type':
            time_field = 'collect_time'
        time_list = self._df_time_block_list(df_ele_fence, time_field)  # 获得处理完数据里时间戳的时间跨度列表，格式为毫秒级时间戳
        self._hdfs_merge_by_time(time_list, output_path, df_ele_fence, time_field=time_field)

    def _hdfs_merge_by_time(self, time_range_list, merge_path, spark_df_new, time_field):
        day_list = [time.strftime("%Y%m%d", time.localtime(int(i / 1000))) for i in time_range_list]
        if not self.HDFS_CLIENT.exists(merge_path):
            self.HDFS_CLIENT.mkdirs(merge_path)
        hdfs_path_list = self.HDFS_CLIENT.listdir(merge_path)
        for i in range(len(day_list) - 1):
            df_new_tmp = spark_df_new[spark_df_new[time_field] > time_range_list[i]][spark_df_new[time_field] < time_range_list[i + 1]]    # df按时间一个一个存入src path
            if day_list[i] not in hdfs_path_list:
                df_new_tmp = df_new_tmp.repartition(1)
                df_new_tmp.write.parquet(merge_path + day_list[i], mode='overwrite')  # src path中无该文件，直接存入
                continue
            spark_df_tmp = spark.read.parquet(merge_path + day_list[i])
            spark_df_tmp = spark_df_tmp.union(df_new_tmp)
            spark_df_tmp.write.parquet(merge_path + day_list[i] + '_bak', mode='overwrite')  # 由于不能读指定文件后又覆写该文件，所以得先建bak
            spark_df_tmp = spark.read.parquet(merge_path + day_list[i] + '_bak')
            spark_df_tmp = spark_df_tmp.repartition(1)
            spark_df_tmp.write.parquet(merge_path + day_list[i], mode='overwrite')
            self.HDFS_CLIENT.delete(merge_path + day_list[i] + '_bak', recursive=True)

    def _df_time_block_list(self, df, time_field):
        one_day_timestamp = 86400000
        df = df.select(time_field)
        df = df.sort(df[time_field])
        time_on = df.first()[0]
        time_on = int(time_on / one_day_timestamp) * one_day_timestamp - 28800000
        df = df.sort(df[time_field].desc())
        time_off = df.first()[0]
        time_off = (int(time_off / one_day_timestamp) + 1) * one_day_timestamp - 28800000
        day_count = int((time_off - time_on) / one_day_timestamp)
        time_range_list = []
        for i in range(day_count + 1):
            time_range_list.append(time_on + i * one_day_timestamp)
        return time_range_list


class SparkUdfSet(object):
    def __init__(self):
        pass

    def udf_rounded_up(self, time_on, span):
        """按指定间隔舍入数据"""
        try:
            time_on = int(time_on)
        except ValueError:
            return 0
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
        s = str(hex(eval(data)))[2:].upper().rjust(12, '0')
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

    def udf_x1000(self, data):
        """某字段所有数字类型数据乘以1000"""
        return int(data) * 1000


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    CompressAndMergeData(spark).compress_data_src(
        '/test/cjh/weihai/src/audit_type',
        '/test/cjh/weihai/compress/audit_type',
        '20201101',
        'audit_type'
    )
    # 'hdfs://192.168.7.150:8020/test/xkx/demo/probe_type/20201127',
    # 'hdfs://192.168.7.150:8020/test/cjh/par/probe_type20201202',
    # '20201127110502594_430300_755652234_001.parquet',
    # 'probe_type'
    spark.stop()