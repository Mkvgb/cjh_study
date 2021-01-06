# -*- encoding: utf-8 -*-
"""
@File       :   st_etl_gd_ele_fence.py
@Contact    :   ggsddu.com
@Modify Time:   2020/8/21 11:19
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import collections
import json
import time
from io import StringIO

import numpy as np
import pandas as pd
from psycopg2.pool import SimpleConnectionPool
from sqlalchemy import create_engine
from loguru import logger



ETL_SCHEMA = 'zhaoqing_duanzhou_db'
# probe_mac, place_code, collect_mac, time_on
probe_fields_change = {"start_time": "time_on"}
wifi_fields_change = {"wifi_mac": "probe_mac",
                      "collect_time": "time_on"}
audit_fields_change = {"net_ending_mac": "probe_mac",
                       "collect_time": "time_on"}
im_fields_change = {"local_id": "probe_mac",
                    "collect_time": "time_on"}
# -*- coding: utf-8 -*-
# @Time : 2020/9/24 17:10
# @Author : XuNanHang
# @File : setting.py
# @Description :
import os

MODE = 'Master'

PROJECT = 'weihai'
DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '6543'
DST_DB_USER = "postgres"
# DST_DB_PASSWORD = "ggsddu@123"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "zhaoqing_duanzhou_db"

RABBIT_MQ_PREFIX = 'master'
RABBIT_MQ_HOST = '192.168.1.99'
RABBIT_MQ_PORT = '5672'
RABBIT_MQ_USER = 'admin'
RABBIT_MQ_PASSWORD = 'admin'
RABBIT_MQ_VIRTUAL_HOST = '/'
RABBIT_MQ_EXCHANGE = f'{RABBIT_MQ_PREFIX}_direct_exchange'
RABBIT_MQ_SCHEDULE_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_schedule_router'
RABBIT_MQ_SCHEDULE_QUEUE = f'{RABBIT_MQ_PREFIX}_scheduler_queue'

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

REDIS_HOST = '192.168.1.99'
REDIS_DB = 1
REDIS_PORT = '6379'
REDIS_PASSWORD = 'ggsddu@police'

HDFS_HOSTS = [
    '192.168.7.150:9870',
    '192.168.7.151:9870',
    '192.168.7.152:9870',
]

SPARK_FILE_PATH = os.path.join(os.path.dirname(__file__), 'sparktask/tasks/')

SMS_PORT = '/dev/ttyUSB0'
SMS_BAUDRATE = 115200
SMS_PIN = '1234'
SMS_QUEUE = 'sms_20201228'

FTP_ROOT_DIR = '/var/ggsddu/data/szgd/'
API_PLACE = 'http://192.168.1.99:8080/police/oauth/place/batch/putOrPost'
API_DEVICE = 'http://192.168.1.99:8080/police/oauth/device/batch/putOrPost'
HDFS_HOST = '192.168.7.150:9870'
HDFS_USER_NAME = 'hdfs'
HDFS_ROOT_DIR = f'/{PROJECT}/src/'

SSH_IP_FILE_SERVER = '192.168.7.152'
SSH_PORT_FILE_SERVER = 22
SSH_USERNAME_FILE_SERVER = 'root'
SSH_PASSWORD_FILE_SERVER = 'ggsddu@518'

LOCAL_TEMP_DIR='/var/ggsddu/data/temp/'

pd.set_option('display.max_columns', None)  # 显示最大列数
pd.set_option('display.width', None)  # 显示宽度
pd.set_option('colheader_justify', 'center')  # 显示居中
pd.set_option('max_colwidth', 200)


def udf_string_float_to_string(data):
    if data.find('.') != -1:
        return data[:data.find('.')]
    return data


def udf_rounded_up(df, span):
    """按指定间隔舍入数据"""
    tmp = span / 2
    if df["time_on"] % span > tmp:
        return int(int(df["time_on"] / span) * span + span)
    elif df["time_on"] % span <= tmp:
        return int(int(df["time_on"] / span) * span)


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


def udf_mac_h2o(data):
    if '-' in data:
        return int(data.replace('-', ''), 10)
    else:
        return data


def udf_null_to_zero(data):
    """输入字符串,为空,返回'0'"""
    if not data:
        return "0"
    else:
        return data


def udf_track_union(time_list, time_stack_span):
    time_list = str(time_list["time_list"])[3: -3].split('],[')
    time_list = [x.split(",") for x in time_list]
    time_list = [[int(i) for i in x] for x in time_list]
    time_list = sorted(time_list)
    # return time_list
    for i in range(len(time_list) - 1):
        if time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1]:  # 新被旧包含
            time_list[i + 1][0] = time_list[i][0]
            time_list[i + 1][1] = time_list[i][1]
            sum = time_list[i][2] + time_list[i + 1][2]
            time_list[i][2] = 0
            time_list[i + 1][2] = sum
        elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1]:  # 旧被新包含
            time_list[i][0] = time_list[i + 1][0]
            time_list[i][1] = time_list[i + 1][1]
            sum = time_list[i][2] + time_list[i + 1][2]
            time_list[i][2] = 0
            time_list[i + 1][2] = sum
        elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1] and time_list[i][0] - \
                time_list[i + 1][1] <= time_stack_span:  # 新在旧左边
            time_list[i][0] = time_list[i + 1][0]
            time_list[i + 1][1] = time_list[i][1]
            sum = time_list[i][2] + time_list[i + 1][2]
            time_list[i][2] = 0
            time_list[i + 1][2] = sum
        elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1] and time_list[i][0] - \
                time_list[i + 1][1] > time_stack_span:
            pass
        elif time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1] and time_list[i + 1][0] - \
                time_list[i][1] <= time_stack_span:  # 新在旧右边
            time_list[i + 1][0] = time_list[i][0]
            time_list[i][1] = time_list[i + 1][1]
            sum = time_list[i][2] + time_list[i + 1][2]
            time_list[i][2] = 0
            time_list[i + 1][2] = sum
        elif time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1] and time_list[i + 1][0] - \
                time_list[i][1] > time_stack_span:
            pass
    time_list_output = []
    for element in time_list:
        if element not in time_list_output and element[2] != 0:
            time_list_output.append(element)
    return str(time_list_output)[2:-2]
    # return time_list


def udf_time_etl(time_list, time_stack_span):
    """
    将没用的数据清洗掉，最后生成轨迹，只剩开始时间与结束时间，并对该时间段的被探测次数进行统计，
    其中参数time_list为[[time_on, count],[]]格式，time_stack_span为生成新轨迹的规定时间间隔
    """
    time_list = str(time_list["time_list"])[3: -3].split('],[')  # .replace('\'', '')
    time_list = [x.split(",") for x in time_list]
    time_list = [[int(i) for i in x] for x in time_list]
    time_list = sorted(time_list)
    lt_len = len(time_list)
    list_tmp = []
    list_e = []
    record_count = 0
    for j in range(lt_len):
        if lt_len == 1:
            list_e.append(time_list[0][0])
            list_e.append(time_list[0][0])
            list_e.append(time_list[0][1])
            _count = 0
            list_tmp.append(str(list_e))
            list_e.clear()
            break
        if j == 0:
            list_e.clear()
            list_e.append(time_list[0][0])
            record_count += time_list[0][1]
            continue
        elif j == lt_len - 1:
            if time_list[j][0] - time_list[j - 1][0] <= time_stack_span:
                list_e.append(time_list[j][0])
                if list_e[0] != list_e[1]:
                    record_count += time_list[j][1]
                list_e.append(record_count)
                _count = 0
                list_tmp.append(str(list_e))
                list_e.clear()
            else:
                list_e.append(time_list[j - 1][0])
                list_e.append(record_count)
                list_tmp.append(str(list_e))
                list_e.clear()

                list_e.append(time_list[j][0])
                list_e.append(time_list[j][0])
                list_e.append(time_list[j][1])
                record_count = 0
                list_tmp.append(str(list_e))
                list_e.clear()
        elif time_list[j][0] - time_list[j - 1][0] > time_stack_span:
            list_e.append(time_list[j - 1][0])
            list_e.append(record_count)
            list_tmp.append(str(list_e))
            list_e.clear()
            list_e.append(time_list[j][0])
            _count = time_list[j][1]
        else:
            record_count += time_list[j][1]

    return str(list_tmp)[3:-3]


class AnalysisEleFence(object):
    def __init__(self):
        self.db_opr = DBOperator()
        # self.hdfs_opr = HDFSManage()
        self.HDFS_TRACK_PATH = "/track/tmp_track_new"
        self.HDFS_TRACK_INFO_FILENAME = "st_track_info"
        self.HDFS_TRACK_FILENAME = "st_track"
        self.HDFS_ST_TRACK_INFO_SUB = '/track/tmp_track_new/st_track_info'
        self.HDFS_ST_TRACK_SUB = "/track/tmp_track_new/st_track"
        self.device_table_name = 'gd_device'
        self.track_table_name = "track"
        self.attr_record_table = 'attr_record'

    def etl_real_time(self, table_name, df_ef, precision_span, time_stack_span):
        """
        对所有的probe_mac进行清洗，入库
        """
        etl_job = AnalysisEleFence()
        attr_df = self.db_opr.read_attr_data()
        if attr_df.empty:
            logger.info(f"ETL->预警规则为空，不予处理")
            return
        # attr_list = clue_rule_df['clue_value'].tolist()     # 判断到预警规则表不为空，才进行后续处理
        # attr中有的mac拉出来，取df中有attr中mac的数据，数据处理完后，将warn_clue中有的mac在df中有的数据拉出来，返回track_id给mq
        attr_list = attr_df['attr_value'].tolist()     # attr中类型为5，同步为true的mac拉出来
        # conn_attr, df_attr = self.db_opr.read_pgsql_to_pandas_dataframe(self.PGSQL_214, self.PGSQL_214_ATTR)
        # df_attr = self.db_opr.read_pgsql_to_pandas_dataframe(self.PGSQL_PROP, self.PGSQL_ATTR)
        # logger.info("preprocess : read_sql success")
        # df_ef = etl_job.prepare_data(df_ef, df_attr)
        # logger.info("preprocess : prepare_data success")
        df_ef, warn_list, mq_flag = etl_job.prepare_data(df_ef, attr_list, table_name)  # 数据格式化，以供后续处理
        # if type(df_ef) == int:  # 如传入的数据为空或数据格式化完后不存在数据，跳出
        #     return 0
        # df_ef["place_code"] = df_ef["place_code"].astype(str)
        # df_ef["place_code"] = df_ef["place_code"].map(udf_string_float_to_string)
        # df_ef_dev = df_ef[["place_code", "collect_mac"]].drop_duplicates(subset=["collect_mac"])     # 对设备进行去重，保存去重后的场所号设备号到df_ef_dev种
        # # ele[probe_mac, place_code, time_start, time_end, count]  --  etl_2_track
        # df_ef = etl_job.etl_2_track(df_ef, precision_span, time_stack_span)     # 压缩轨迹，时间约数为precision_span，新轨迹间隔为time_stack_span
        # logger.info(f"ETL->etl_2_track success"),
        # if type(df_ef) == int:
        #     return 0
        # # 当前旧轨迹表数据从atrack表中取，并转存到atrack_tmp中，最后存回atrack，后续得改为到hdfs中取
        # df_track_source = etl_job.union_track_2_hdfs(df_ef, time_stack_span)
        # logger.info(f"ETL->union_track_2_hdfs success")
        # if type(df_track_source) == int: return 0
        # df_track_output = etl_job.standardize_etl_data(df_track_source)
        # logger.info(f"ETL->output success")
        # df_track_append, track_id = etl_job.update_track_table(df_track_output, df_ef_dev, warn_list)
        # logger.info(f"ETL->`update_track_talbe` success")
        # if type(df_track_append) == int: return 0
        # attr_df = self.db_opr.update_attr(df_track_append)
        # etl_job.update_attr_record(df_track_append, attr_df, track_id, mq_flag)
        # logger.info(f"ETL->update_attr_record success")

    def prepare_data(self, df_ef, attr_list, table_name):
        """
        数据准备工作，为正式处理前进行数据过滤
        只操作attr表中有的probe_mac，并选取后续所需字段
        """
        if table_name == 'probe_type':  # 对4种类型的数据，进行字段转换
            df_ef = df_ef.rename(columns=probe_fields_change)
        elif table_name == 'wifi_type':
            df_ef = df_ef.rename(columns=wifi_fields_change)
        elif table_name == 'audit_type':
            df_ef = df_ef.rename(columns=audit_fields_change)
        elif table_name == 'im_type':
            df_ef = df_ef.rename(columns=im_fields_change)
        else:
            return 1, False, False
        df_ef = df_ef[df_ef['probe_mac'].isin(attr_list)]   # 判断数据种是否包含预警mac，没有则不执行后续

        warn_clue_df = self.db_opr.read_clue_rule_data()
        warn_list = warn_clue_df['clue_value'].tolist()
        mq_flag = 0
        if not df_ef[df_ef['probe_mac'].isin(warn_list)].empty:
            mq_flag = 1

        if df_ef.empty:
            logger.info(f"ETL->没有预警数据,任务结束")
            return df_ef, False, False
        else:
            logger.info(f"ETL->发现预警消息")
        # df_ef["probe_mac"] = df_ef["probe_mac"].map(udf_mac_o2h)
        # df_attr = df_attr[df_attr["attr_type_id"] == 5]
        # if len(df_attr) == 0: return len(df_attr)
        # df_attr = df_attr[["attr_value"]].rename(columns={"attr_value": "probe_mac"})
        # 只选出attr表中有的probe_mac进行处理
        # df_ef = pd.merge(df_attr, df_ef, on="probe_mac", how="left")
        df_ef = df_ef[['time_on', 'probe_mac', 'place_code', 'collect_mac']]    # 数据只选取探测时间time_on，探测mac probe_mac，场所号place_code，采集设备collect_mac
        # df_ef = df_ef.dropna(how="any")
        # if len(df_ef) == 0: return len(df_ef)
        df_ef["place_code"] = df_ef["place_code"].map(udf_null_to_zero)
        df_ef = df_ef[df_ef["place_code"] != "0"]
        print(df_ef)
        return df_ef, warn_list, mq_flag

    # def etl_2_track(self, df_ef, precision_span, time_stack_span):
    #     """
    #     对实时获取到的电子围栏数据进行清洗，形成轨迹
    #     具体格式为[probe_mac, place_code, time_start, time_end, count]
    #     """
    #     df_ef["time_on"] = df_ef.apply(udf_rounded_up, axis=1, args=(precision_span,))  # 对数据时间进行统计处理，约为间隔为60s
    #     df_ef = df_ef.groupby(["probe_mac", "place_code", "time_on"]).agg({"time_on": "count"}).rename(
    #         columns={"time_on": "count"}).reset_index()         # 相同probe_mac，place_code，time_on的数据，对time_on进行count
    #     df_ef = df_ef.groupby(["probe_mac", "place_code", "time_on"]).agg({"count": "max"}).rename(
    #         columns={"count": "max_count"}).reset_index()       # 相同probe_mac，place_code，time_on的数据，对刚刚生成的count进行取最大
    #     df_ef["max_count"] = df_ef["max_count"].astype(str)
    #     df_ef["time_on"] = df_ef["time_on"].astype(str)
    #     df_ef['time_on'] = df_ef["time_on"] + ',' + df_ef["max_count"]  # 将time_on与max_count合并
    #     df_ef = df_ef[["time_on", "probe_mac", "place_code"]]
    #     df_ef["time_on"] = df_ef["time_on"].map(lambda x: "[" + x + "]")
    #     # 将相同probe_mac和place_code的time_on(time_on, max_count)合并成list，具体为time_on = [[time_on,max_count],[time_on,max_count],...]
    #     df_ef = df_ef.groupby(["probe_mac", "place_code"])["time_on"].apply(
    #         lambda time_on: [','.join(time_on)]).reset_index().rename(columns={"time_on": "time_list"})
    #     # 删除没用的数据并按一定规则生成轨迹（开始时间+结束时间）
    #     # ele[probe_mac, place_code, time_list], 其中time_list = 12,13,1]', '[13,14,2]', '[14,15,3
    #     df_ef["time_list"] = df_ef.apply(udf_time_etl, axis=1, args=(time_stack_span,))
    #     # df_ef["time_list"] = df_ef["time_list"].map(lambda x: str(x.replace(" ", "").replace("'")))
    #     if len(df_ef) == 0: return 0
    #     df_ef = df_ef.drop(["time_list"], axis=1).join(
    #         df_ef["time_list"].str.split("\\]', '\\[", expand=True).stack().reset_index(level=1, drop=True).rename(
    #             "time_list"))
    #     df_ef["time_list"] = df_ef["time_list"].map(lambda x: x.replace(" ", ""))
    #     df_ef = df_ef.join(df_ef["time_list"].str.split(',', expand=True)).rename(
    #         columns={0: "time_start", 1: "time_end", 2: "count"})
    #     df_ef = df_ef.drop(['time_list'], axis=1).drop_duplicates()
    #     df_ef["time_start"] = df_ef["time_start"].astype("long")
    #     df_ef["time_end"] = df_ef["time_end"].astype("long")
    #     df_ef["count"] = df_ef["count"].astype("long")
    #     return df_ef
    #
    # def union_track_2_hdfs(self, df_ef, time_stack_span):
    #     """
    #     将hdfs中的旧track数据与新的track数据进行合并，进行轨迹合并操作，将合并后的新的轨迹存回hdfs
    #     """
    #     df_ef["place_code"] = df_ef["place_code"].astype(str)
    #     # ele[probe_mac, place_code, time_start, time_end, count]
    #     # 如果旧轨迹表有数据，则拉过来进行union并在后面进行轨迹合并
    #     if self.hdfs_opr.check_path_is_exist(self.HDFS_ST_TRACK_SUB):
    #         # [probe_mac, place_code, time_start, time_end, count]
    #         df_track = self.hdfs_opr.read_csv_to_df(self.HDFS_ST_TRACK_SUB)
    #         df_track["time_start"] = df_track["time_start"].astype("long")
    #         df_track["time_end"] = df_track["time_end"].astype("long")
    #         df_track["count"] = df_track["count"].astype("long")
    #         df_ef = pd.concat([df_ef, df_track], axis=0)
    #     else:
    #         logger.info(f"ETL->client ready to create")
    #         self.hdfs_opr.push_csv_data(
    #             hdfs_dir=self.HDFS_TRACK_PATH,
    #             filename=self.HDFS_TRACK_FILENAME,
    #             df=df_ef
    #         )
    #         logger.info(f"ETL->new data success")
    #         return df_ef
    #     logger.info(f"ETL->hdfs connect success")
    #     df_ef = df_ef.sort_values("time_start").reset_index().drop(["index"], axis=1)
    #     df_ef["time_start"] = df_ef["time_start"].astype(str)
    #     df_ef["time_end"] = df_ef["time_end"].astype(str)
    #     df_ef["count"] = df_ef["count"].astype(str)
    #     df_ef["time_on"] = df_ef["time_start"] + ',' + df_ef["time_end"] + ',' + df_ef["count"]
    #     df_ef = df_ef[["time_on", "probe_mac", "place_code"]]
    #     df_ef["time_on"] = df_ef["time_on"].map(lambda x: "[" + x + "]")
    #     df_ef = df_ef.groupby(["probe_mac", "place_code"])["time_on"].apply(
    #         lambda time_on: [','.join(time_on)]).reset_index().rename(columns={"time_on": "time_list"})
    #     # ele[probe_mac, place_code, time_list[time_on(time_start, time_end, count), time_on(time_start, time_end, count), ...]
    #     df_ef["time_list"] = df_ef.apply(udf_track_union, axis=1, args=(time_stack_span,))
    #     df_ef["time_list"] = df_ef["time_list"].map(lambda x: str(
    #         x.replace(" ", "")))  # .replace("'", "").split("\\],\\["))[1: -1].replace("]", "").replace("[", "")
    #     df_ef = df_ef.drop(["time_list"], axis=1).join(
    #         df_ef["time_list"].str.split("\\],\\[", expand=True).stack().reset_index(level=1, drop=True).rename(
    #             "time_list"))
    #     df_ef = df_ef.reset_index()
    #     df_ef = df_ef.join(df_ef["time_list"].str.split(',', expand=True)).rename(
    #         columns={0: "time_start", 1: "time_end", 2: "count"}).drop(["time_list"], axis=1)
    #     if len(df_ef) == 0:
    #         return len(df_ef)
    #     if len(df_ef) > 0:
    #         self.hdfs_opr.delete_path(path=self.HDFS_ST_TRACK_SUB)
    #         self.hdfs_opr.push_csv_data(
    #             hdfs_dir=self.HDFS_TRACK_PATH,
    #             filename=self.HDFS_TRACK_FILENAME,
    #             df=df_ef
    #         )
    #     df_ef = df_ef[["probe_mac", "place_code", "time_start", "time_end"]]
    #     df_track = df_track[["probe_mac", "place_code", "time_start", "time_end"]]
    #     df_track["probe_mac"] = df_track["probe_mac"].astype(str)
    #     df_track["place_code"] = df_track["place_code"].astype(str)
    #     df_track["time_start"] = df_track["time_start"].astype(str)
    #     df_track["time_end"] = df_track["time_end"].astype(str)
    #     df_ef = df_ef.append(df_track).append(df_track).drop_duplicates(
    #         subset=["probe_mac", "place_code", "time_start", "time_end"], keep=False)
    #     if len(df_ef) == 0: return 0
    #     return df_ef
    #
    # def standardize_etl_data(self, df_track_source):
    #     """
    #     标准化清洗完的数据，并转为pgsql中track表的格式，供后续更新
    #     """
    #     df_track_source["time_start"] = df_track_source["time_start"].astype(str)
    #     df_track_source["time_end"] = df_track_source["time_end"].astype(str)
    #     df_track_source["time_start"] = df_track_source["time_start"].map(lambda x: x + ',0')
    #     df_track_source["time_end"] = df_track_source["time_end"].map(lambda x: x + ',1')
    #     df_track_source["probe_time"] = df_track_source["time_start"] + "-" + df_track_source["time_end"]
    #     df_track_source = df_track_source[["probe_time", "probe_mac", "place_code"]].reset_index().drop(["index"],
    #                                                                                                     axis=1)
    #     df_track_source = df_track_source.drop(["probe_time"], axis=1).join(
    #         df_track_source["probe_time"].str.split("-", expand=True).stack().reset_index(level=1, drop=True).rename(
    #             "probe_time"))  # .drop_duplicates()
    #     df_track_source = df_track_source.reset_index().drop(["index"], axis=1)
    #     df_track_source = df_track_source.join(df_track_source["probe_time"].str.split(',', expand=True)).drop(
    #         ["probe_time"], axis=1).rename(columns={0: "probe_time", 1: "flag"})
    #     df_track_source = df_track_source.rename(columns={"probe_mac": "probe_data"})
    #     df_track_source["datasource_table_name"] = "probe_type"
    #     df_track_source["flag"] = df_track_source["flag"].map(udf_null_to_zero)
    #     return df_track_source
    #
    # def update_track_table(self, df_track_output, df_ef_dev, warn_list):
    #     """
    #     执行更新操作，将需要更新的数据列出，将新的数据追加到track表中
    #     """
    #     if self.hdfs_opr.check_path_is_exist(self.HDFS_ST_TRACK_INFO_SUB):
    #         try:
    #             df_track_old = self.hdfs_opr.read_csv_to_df(self.HDFS_ST_TRACK_INFO_SUB)# , '1'
    #             df_track_old = df_track_old[
    #                 ["probe_time", "probe_data", "place_code", "flag", "datasource_table_name"]]
    #         except:
    #             self.hdfs_opr.delete_path(path=self.HDFS_ST_TRACK_INFO_SUB)
    #             df_track_old = pd.DataFrame(
    #                 columns=['probe_time', 'probe_data', 'place_code', 'flag', 'datasource_table_name'])
    #     else:
    #         logger.info(f"ETL->st_track_info ready to create")
    #         df_track_old = pd.DataFrame(
    #             columns=['probe_time', 'probe_data', 'place_code', 'flag', 'datasource_table_name'])
    #     logger.info(f"ETL->update_track client init success")
    #     df_track_old = df_track_old[df_track_old["datasource_table_name"] == "probe_type"]
    #     df_track_append = df_track_output.append(df_track_old).append(df_track_old).drop_duplicates(keep=False)
    #     df_track_append["create_time"] = int(time.time() * 1000)
    #     df_track_append["datasource_id"] = range(len(df_track_append))
    #     # df_track_append["probe_time"] = df_track_append["probe_time"].astype("long").map(lambda x: x * 1000)
    #     df_ef_dev["place_code"] = df_ef_dev["place_code"].astype(str)
    #
    #     # df_ef_dev["collect_mac"] = df_ef_dev["collect_mac"].astype(np.int64)  # 已经是16进制类型了
    #     # df_ef_dev["collect_mac"] = df_ef_dev["collect_mac"].map(udf_mac_o2h)
    #     df_track_append['place_code'] = df_track_append['place_code'].astype(str)
    #     df_track_append = pd.merge(df_track_append, df_ef_dev, on="place_code", how="left")
    #     # conn_dev, df_dev = self.db_opr.read_pgsql_to_pandas_dataframe(self.PGSQL_214, self.PGSQL_214_DEV)
    #     logger.info(f"ETL->ready to exec read_pgsql_to_pandas_dataframe")
    #     df_dev = self.db_opr.read_pgsql_to_pandas_dataframe(self.device_table_name)
    #     df_dev = df_dev.rename(columns={'device_mac': 'collect_mac'})
    #     logger.info(f"ETL->exec read_pgsql_to_pandas_dataframe finish")
    #     df_track_append = pd.merge(df_track_append, df_dev[["id", "collect_mac"]], how="left", on="collect_mac")
    #     df_track_append = df_track_append.rename(columns={"id": "probe_device_id"})
    #     df_probe_data = df_track_append[["datasource_id", "probe_data"]]
    #     df_track_append.drop(["flag", "probe_data"], axis=1)
    #     if len(df_track_append) == 0:
    #         return 0, None
    #     logger.info(f"ETL->st_track_info ready to append")
    #     if self.hdfs_opr.check_path_is_exist(self.HDFS_ST_TRACK_INFO_SUB):
    #         self.hdfs_opr.delete_path(path=self.HDFS_ST_TRACK_INFO_SUB)
    #     self.hdfs_opr.push_csv_data(
    #         hdfs_dir=self.HDFS_TRACK_PATH,
    #         filename=self.HDFS_TRACK_INFO_FILENAME,
    #         df=df_track_append.append(df_track_old)
    #     )
    #
    #     logger.info(f"ETL->st_track_info append success")
    #     df_track_append = df_track_append[
    #         ["probe_time", "place_code", "create_time", "datasource_id", "datasource_table_name", "probe_device_id"]]
    #     df_track_append["datasource_table_name"] = df_track_append["datasource_table_name"].map(
    #         lambda x: '\'' + x + '\'')
    #     df_track_append["place_code"] = df_track_append["place_code"].map(lambda x: '\'' + x + '\'')
    #     df_track_append = df_track_append.where(df_track_append.notnull(), None)
    #     df_track_append = df_track_append[~(df_track_append['probe_device_id'].isnull())].reset_index(drop=True)
    #     logger.info("写入track的数据")
    #     logger.info(df_track_append)
    #     if df_track_append.empty:
    #         return 0, None
    #     id_list = self.db_opr.pg_insert_return_id(
    #         self.track_table_name,
    #         ["probe_time", "place_code", "create_time", "datasource_id",
    #          "datasource_table_name", "probe_device_id"],
    #         pandas_dataframe_to_string_sql_insert_values(df_track_append)
    #     )
    #     logger.info(f"ETL->df_track_append_insert_sql success")
    #     pd_id_list = pd.DataFrame(id_list, columns=["track_id"])
    #     df_track_append["track_id"] = pd_id_list["track_id"]
    #     df_track_append = pd.merge(df_track_append, df_probe_data, on=["datasource_id"], how="left")
    #     warn_df = df_track_append[df_track_append['probe_data'].isin(warn_list)]
    #     return df_track_append, warn_df['track_id'].tolist()
    #
    # def update_attr_record(self, df_track_append, df_attr, track_id, mq_flag):
    #     # df_track_append [place_code, probe_time, probe_data, flag, datasource_table_name, create_time, datasource_id, probe_device_id]
    #     # df_track [id, probe_time, place_code, create_time, datasource_id, datasource_table_name, base_person_id, probe_device_id]
    #     # 太多，后续无法读取
    #     # df_attr = df_attr[df_attr["attr_type_id"] == 5]
    #     # df_attr = df_attr[["id", "attr_value"]].rename(columns={"attr_value": "probe_data"})
    #     df_track_append = pd.merge(df_track_append, df_attr, on=["probe_data"], how="left")
    #     df_track_append = df_track_append.rename(columns={"id": "attr_id"})
    #     df_track_append["create_time"] = int(time.time() * 1000)
    #     df_track_append = df_track_append[["track_id", "attr_id", "create_time"]]
    #     logger.info("以下是attr_record数据")
    #     logger.info(df_track_append)
    #     logger.info(f"ETL->start to insert")
    #     self.db_opr.pg_insert_return_id(self.attr_record_table,
    #                                     ["track_id", "attr_id", "create_time"],
    #                                     pandas_dataframe_to_string_sql_insert_values(df_track_append))
    #     logger.info(f"ETL->insert finish")
    #     # if mq_flag:
    #     #     publisher = RabbitPublisher(
    #     #         exchange=RABBIT_MQ_EXCHANGE,
    #     #         route_key=RABBIT_MQ_PERSON_TRACK_ROUTE_KEY,
    #     #         queue=RABBIT_MQ_PERSON_TRACK_QUEUE
    #     #     )
    #     #     publisher.publish(json.dumps(track_id))


def decimal_two_place(val):
    from decimal import Decimal
    decimal_val = Decimal(val).quantize(Decimal('0.00'))
    return decimal_val


def decimal_four_place(val):
    from decimal import Decimal
    decimal_val = Decimal(val).quantize(Decimal('0.0000'))
    return decimal_val

class ETL(object):

    def on_mq_entry(self, deliver_data, precision_span, time_stack_span):

        transfer_df = pd.DataFrame(deliver_data.get('content', None))
        table_name = deliver_data.get('dataSrc', None)
        logger.info(f"ETL->datasync start")
        start_time = time.time()
        try:
            AnalysisEleFence().etl_real_time(
                table_name,
                transfer_df,
                precision_span,
                time_stack_span,
            )
        except Exception as e:
            logger.exception(f"ETL->发生错误 {e}")
        emd_time = time.time()
        logger.info(f"ETL->耗时{decimal_four_place(emd_time - start_time)}秒")


class DBOperator(object):
    def __init__(self):
        self.db_conn_pool = SimpleConnectionPool(
            2,
            3,
            host=DST_DB_HOST,
            port=int(DST_DB_PORT),
            user=DST_DB_USER,
            password=DST_DB_PASSWORD,
            database=DST_DB_DATABASE
        )

    def pg_insert_return_id(self, table_name, field_list, insert_values):
        field_list = ','.join(field_list)
        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        if table_name == 'attr_record':
            cursor.execute(
                f"SELECT setval('{ETL_SCHEMA}.person_attr_record_id_seq', (SELECT max(id) FROM {ETL_SCHEMA}.attr_record));")
        cursor.execute(
            f"insert into {ETL_SCHEMA}.{table_name} ({field_list}) values {insert_values} RETURNING id")
        lt = cursor.fetchall()
        lt = [list(x) for x in lt]
        conn.commit()
        cursor.close()
        self.db_conn_pool.putconn(conn)
        return lt

    def append_pgsql(self, df, prop_info, tb_info):
        """table='atest' , schema='analysis_etl_gd_ele_fence'"""
        engine = create_engine(
            f"postgresql://{prop_info['user']}:{prop_info['password']}@{prop_info['host']}:{prop_info['port']}/{tb_info['database']}",
            max_overflow=0, pool_size=5, pool_timeout=30, pool_recycle=-1)

        pd_sql_engine = pd.io.sql.pandasSQL_builder(engine)
        pd_table = pd.io.sql.SQLTable(tb_info["tablename"], pd_sql_engine, frame=df, index=False, if_exists="append",
                                      schema=tb_info["schema"])
        pd_table.create()
        sio = StringIO()
        df.to_csv(sio, sep='|', encoding='utf-8', index=False)
        sio.seek(0)
        conn = self.db_conn_pool.getconn()
        cursor = conn.cursor()
        copy_cmd = f"COPY {tb_info['schema']}.{tb_info['tablename']} FROM STDIN HEADER DELIMITER '|' CSV"
        cursor.copy_expert(copy_cmd, sio)
        conn.commit()
        self.db_conn_pool.putconn(conn)

    def read_pgsql_to_pandas_dataframe(self, table_name):
        conn = self.db_conn_pool.getconn()
        try:
            df = pd.concat(
                pd.read_sql(
                    f'''select * from {ETL_SCHEMA}.{table_name}''',
                    con=conn,
                    chunksize=1000
                )
            )
        except (TypeError, ValueError):
            df = pd.read_sql(
                f'''select * from {ETL_SCHEMA}.{table_name}''',
                con=conn
            )
        self.db_conn_pool.putconn(conn)
        return df

    def read_clue_rule_data(self):
        conn = self.db_conn_pool.getconn()
        df = pd.read_sql(
            f"select clue_value from {ETL_SCHEMA}.warn_clue_rule where " +
            f"clue_type = 'mac' and start_time <= now() and end_time >= now()",
            con=conn
        )
        self.db_conn_pool.putconn(conn)
        return df

    def read_attr_data(self):
        conn = self.db_conn_pool.getconn()
        df = pd.read_sql(
            f"select attr_value from {ETL_SCHEMA}.attr where " +
            f"attr_type_id = 5 and sync_in_time = 't'",
            con=conn
        )
        self.db_conn_pool.putconn(conn)
        return df

    def update_attr(self, df):
        """
        检查是否需要生成新的attr记录
        """
        if not df.empty:
            attr_mac_list = tuple(set(df['probe_data'].tolist()))
            if len(attr_mac_list) > 1:
                tmp_attr_mac_list = str(attr_mac_list)
            else:   # ==1
                tmp_attr_mac_list = str(attr_mac_list).replace(',', '')
            current_ts = int(time.time() * 1000)
            conn = self.db_conn_pool.getconn()
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE {ETL_SCHEMA}.attr SET update_time = {current_ts} WHERE attr_type_id = 5 and attr_value in {tmp_attr_mac_list} RETURNING id,attr_value"
            )
            conn.commit()
            query_result = cursor.fetchall()
            exist_attr_df = None
            insert_attr_df = None
            if query_result:
                exist_attr_df = pd.DataFrame(query_result, columns=['id', 'probe_data'])
                insert_mac_list = list((collections.Counter(attr_mac_list) - collections.Counter(
                    exist_attr_df['probe_data'].tolist())).elements())
            else:
                insert_mac_list = attr_mac_list
            insert_df = pd.DataFrame(insert_mac_list, columns=['attr_value'])
            if not insert_df.empty:
                insert_df['attr_type_id'] = 5
                insert_df['create_time'] = current_ts
                insert_df['update_time'] = current_ts
                logger.info("以下是attr数据")
                logger.info(insert_df)
                insert_data = insert_df.to_dict(orient="records")
                insert_data_str = ""
                for data in insert_data:
                    insert_data_str += f"({data['attr_type_id']},'{data['attr_value']}',{data['create_time']},{data['update_time']}),"
                insert_data_str = insert_data_str[:-1]
                cursor.execute(
                    f"SELECT setval('{ETL_SCHEMA}.attr_id_seq', (SELECT max(id) FROM zhaoqing_duanzhou_db.attr));")
                cursor.execute(
                    f"insert into {ETL_SCHEMA}.attr (attr_type_id,attr_value,create_time,update_time) values {insert_data_str} RETURNING id,attr_value")

                conn.commit()
                insert_query_result = cursor.fetchall()
                insert_attr_df = pd.DataFrame(insert_query_result, columns=['id', 'probe_data'])

            self.db_conn_pool.putconn(conn)
            if exist_attr_df is not None and insert_attr_df is not None:
                attr_df = pd.concat([exist_attr_df, insert_attr_df])
                return attr_df
            elif exist_attr_df is not None:
                return exist_attr_df
            else:
                return insert_attr_df


def pandas_dataframe_to_string_sql_insert_values(df):
    lt = df.values.tolist()
    lt = [[str(i) for i in x] for x in lt]
    lt = ['(' + (','.join(x)) + ')' for x in lt]
    slt = ','.join(lt)
    return slt

AnalysisEleFence().etl_real_time()