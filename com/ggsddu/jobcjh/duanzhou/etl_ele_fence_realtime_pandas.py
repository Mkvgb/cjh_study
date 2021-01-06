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
import pandas as pd
import requests
from sqlalchemy import create_engine, Column
from io import StringIO
import numpy as np

from sqlalchemy.orm import sessionmaker


def read_pgsql_to_pandas_dataframe(pginfo, tbinfo):
    conn = f'postgresql+psycopg2://{pginfo["user"]}:{pginfo["password"]}@{pginfo["host"]}:{pginfo["port"]}/{tbinfo["database"]}'
    return conn, pd.concat(pd.read_sql(f'''select * from {tbinfo["schema"]}.{tbinfo["tablename"]}''', con=conn, chunksize=1000))  # chunksize可以实现大文件分块读，且一定得加


def client_init(hosts):
    session = requests.session()
    session.keep_alive = False
    client = pyhdfs.HdfsClient(
        hosts=hosts,
        user_name="hdfs",
        requests_session=session)
    return client


def read_hdfs_csv_to_pandas_dataframe(client, filepath):
    csv = client.open(filepath)
    lt = csv.read().decode().split("\n")
    lt = [x.split(",") for x in lt]
    return pd.DataFrame(data=lt[1:], columns=lt[0])


def pandas_dataframe_to_string_csv(df):
    fs = ','.join(df.columns.values)
    lt = df.values.tolist()
    lt = [[str(i) for i in x] for x in lt]
    lt = [','.join(x) for x in lt]
    slt = '\n'.join(lt)
    return fs + '\n' + slt


def pandas_dataframe_to_string_sql_insert_values(df):
    # fs = ','.join(df.columns.values)
    lt = df.values.tolist()
    lt = [[str(i) for i in x] for x in lt]
    lt = ['(' + (','.join(x)) + ')' for x in lt]
    slt = ','.join(lt)
    return slt


def pg_insert_return_id(prop_info, tb_info, field_list, insert_values):
    field_list = ','.join(field_list)
    engine = create_engine(
        f"postgresql://{prop_info['user']}:{prop_info['password']}@{prop_info['host']}:{prop_info['port']}/{tb_info['database']}",
        max_overflow=0, pool_size=5, pool_timeout=30, pool_recycle=-1)
    dbsession = sessionmaker(bind=engine)
    session = dbsession()
    result = session.execute(f"insert into {tb_info['schema']}.{tb_info['tablename']} ({field_list}) values {insert_values} RETURNING id")
    lt = result.fetchall()
    lt = [list(x) for x in lt]
    session.commit()
    session.close()
    return lt


def append_to_sql(df, prop_info, tb_info, sep=',', encoding='utf8'):
    engine = create_engine(
        f"postgresql://{prop_info['user']}:{prop_info['password']}@{prop_info['host']}:{prop_info['port']}/{tb_info['database']}",
        max_overflow=0, pool_size=5, pool_timeout=30, pool_recycle=-1)
    # # Create Table
    # df[:0].to_sql(tb_info['tablename'], engine, if_exists=if_exists)
    # Prepare data
    output = StringIO()
    df.to_csv(output, sep=sep, header=False, encoding=encoding)
    output.seek(0)
    # Insert data
    connection = engine.raw_connection()
    cursor = connection.cursor()
    print(output.readline())
    cursor.copy_from(output, tb_info['tablename'], sep=sep, null='')
    connection.commit()
    cursor.close()
    connection.close()


def append_pgsql(df, prop_info, tb_info):
    """table='atest' , schema='analysis_etl_gd_ele_fence'"""
    engine = create_engine(
        f"postgresql://{prop_info['user']}:{prop_info['password']}@{prop_info['host']}:{prop_info['port']}/{tb_info['database']}",
        max_overflow=0, pool_size=5, pool_timeout=30, pool_recycle=-1)
    sio = StringIO()
    df.to_csv(sio, sep='|', index=False)
    pd_sql_engine = pd.io.sql.pandasSQL_builder(engine)
    pd_table = pd.io.sql.SQLTable(tb_info["tablename"], pd_sql_engine, frame=df, index=False, if_exists="append", schema=tb_info["schema"])
    pd_table.create()
    sio.seek(0)
    with engine.connect() as connection:
        with connection.connection.cursor() as cursor:
            copy_cmd = f"COPY {tb_info['schema']}.{tb_info['tablename']} FROM STDIN HEADER DELIMITER '|' CSV"
            cursor.copy_expert(copy_cmd, sio)
        connection.connection.commit()


class AnalysisEleFence(object):
    def __init__(self):
        self.HDFS_ST_TRACK_INFO_SUB = '/track/tmp_track_new/st_track_info'
        self.HDFS_ST_TRACK_SUB = "/track/tmp_track_new/st_track"
        self.PGSQL_214 = {'host': '192.168.9.214', 'port': 5432, 'user': 'postgres', 'password': 'postgres'}
        self.PGSQL_160 = {'host': '192.168.7.160', 'port': 5432, 'user': 'postgres', 'password': 'postgres'}
        self.PGSQL_214_DEV = {'database': 'police_analysis_db', 'schema': 'zhaoqing_duanzhou_db', 'tablename': 'gd_device'}
        self.PGSQL_214_ATTR = {'database': 'police_analysis_db', 'schema': 'zhaoqing_duanzhou_db', 'tablename': 'attr'}
        self.PGSQL_214_TRACK = {'database': 'police_analysis_db', 'schema': 'zhaoqing_duanzhou_db', 'tablename': 'track'}
        self.PGSQL_214_ATTR_RECORD = {'database': 'police_analysis_db', 'schema': 'zhaoqing_duanzhou_db', 'tablename': 'attr_record'}
        self.PYHDFS_HOST = "192.168.7.150:9870"

    def etl_real_time(self, file_path, precision_span, timestack_span):
        """对所有的mobile_mac进行清洗，并入库"""
        # 读取实时拉取到的数据
        # df_ef = pd.read_csv('D:/code/cjh_study/test.csv')
        df_ef = pd.read_csv('D:/code/st_analysis_2_pandas/st_analysis/1.csv')
        etl_job = AnalysisEleFence()
        # ele[time_on,mobile_mac,netbar_wacode,collection_equipment_mac]  --  prepare_data
        conn_attr, df_attr = read_pgsql_to_pandas_dataframe(self.PGSQL_214, self.PGSQL_214_ATTR)
        df_ef = etl_job.prepare_data(df_ef, df_attr)
        if type(df_ef) == int: return 0
        df_ef["netbar_wacode"] = df_ef["netbar_wacode"].astype(str)
        df_ef["netbar_wacode"] = df_ef["netbar_wacode"].map(UdfUtils.udf_string_float_to_string)
        df_ef_dev = df_ef.rename(columns={"collection_equipment_mac": "device_mac", "local_mac": "device_mac", "place_code": "netbar_wacode", "router_mac": "device_mac"})
        df_ef_dev = df_ef_dev[["netbar_wacode", "device_mac"]].drop_duplicates(subset=["netbar_wacode"])
        # ele[mobile_mac, netbar_wacode, time_start, time_end, count]  --  etl_2_track
        df_ef = etl_job.etl_2_track(df_ef, precision_span, timestack_span)
        if type(df_ef) == int: return 0
        # 当前旧轨迹表数据从atrack表中取，并转存到atrack_tmp中，最后存回atrack，后续得改为到hdfs中取
        df_track_source = etl_job.union_track_2_hdfs(df_ef, timestack_span)
        if type(df_track_source) == int: return 0
        df_track_output = etl_job.standardize_etl_data(df_track_source)
        df_track_append = etl_job.update_track_talbe(df_track_output, df_ef_dev)
        if type(df_track_append) == int: return 0
        etl_job.update_attr_record(df_track_append, df_attr)

    def prepare_data(self, df_ef, df_attr):
        """数据准备工作，为正式处理前进行数据过滤，只操作attr表中有的mobile_mac，并选取后续所需字段"""
        df_ef = df_ef.rename(columns={"ap_mac": "mobile_mac", "place_code": "netbar_wacode", "local_mac": "collection_equipment_mac", "router_mac": "collection_equipment_mac"})
        df_ef["mobile_mac"] = df_ef["mobile_mac"].map(UdfUtils.udf_mac_o2h)
        df_attr = df_attr[df_attr["attr_type_id"] == 5]
        if len(df_attr) == 0: return len(df_attr)
        df_attr = df_attr[["attr_value"]].rename(columns={"attr_value": "mobile_mac"})
        # 只选出attr表中有的mobile_mac进行处理
        df_ef = pd.merge(df_attr, df_ef, on="mobile_mac", how="left")
        df_ef = df_ef[['time_on', 'mobile_mac', 'netbar_wacode', 'collection_equipment_mac']]
        df_ef = df_ef.dropna(how="any")
        if len(df_ef) == 0: return len(df_ef)
        df_ef["netbar_wacode"] = df_ef["netbar_wacode"].map(UdfUtils.udf_null_to_zero)
        df_ef = df_ef[df_ef["netbar_wacode"] != "0"]
        return df_ef

    def etl_2_track(self, df_ef, precision_span, timestack_span):
        """对实时获取到的电子围栏数据进行清洗，形成轨迹，具体格式为[mobile_mac, netbar_wacode, time_start, time_end, count]"""
        df_ef["time_on"] = df_ef.apply(UdfUtils.udf_rounded_up, axis=1, args=(precision_span,))
        df_ef = df_ef.groupby(["mobile_mac", "netbar_wacode", "time_on"]).agg({"time_on": "count"}).rename(columns={"time_on": "count"}).reset_index()
        df_ef = df_ef.groupby(["mobile_mac", "netbar_wacode", "time_on"]).agg({"count": "max"}).rename(columns={"count": "max_count"}).reset_index()
        df_ef["max_count"] = df_ef["max_count"].astype(str)
        df_ef["time_on"] = df_ef["time_on"].astype(str)
        df_ef['time_on'] = df_ef["time_on"] + ',' + df_ef["max_count"]
        df_ef = df_ef[["time_on", "mobile_mac", "netbar_wacode"]]
        df_ef["time_on"] = df_ef["time_on"].map(lambda x: "[" + x + "]")
        # 将相同mobile_mac和netbar_wacode的time_on(time_on, max_count)合并成list，具体为time_on = [[time_on,max_count],[time_on,max_count],...]
        df_ef = df_ef.groupby(["mobile_mac", "netbar_wacode"])["time_on"].apply(lambda time_on: [','.join(time_on)]).reset_index().rename(columns={"time_on": "time_list"})
        # 删除没用的数据并按一定规则生成轨迹（开始时间+结束时间）
        # ele[mobile_mac, netbar_wacode, time_list], 其中time_list = 12,13,1]', '[13,14,2]', '[14,15,3
        df_ef["time_list"] = df_ef.apply(UdfUtils.udf_time_etl, axis=1, args=(timestack_span,))
        # df_ef["time_list"] = df_ef["time_list"].map(lambda x: str(x.replace(" ", "").replace("'")))
        if len(df_ef) == 0: return 0
        df_ef = df_ef.drop(["time_list"], axis=1).join(df_ef["time_list"].str.split("\\]', '\\[", expand=True).stack().reset_index(level=1, drop=True).rename("time_list"))
        df_ef["time_list"] = df_ef["time_list"].map(lambda x: x.replace(" ", ""))
        df_ef = df_ef.join(df_ef["time_list"].str.split(',', expand=True)).rename(columns={0: "time_start", 1: "time_end", 2: "count"})
        df_ef = df_ef.drop(['time_list'], axis=1).drop_duplicates()
        df_ef["time_start"] = df_ef["time_start"].astype("long")
        df_ef["time_end"] = df_ef["time_end"].astype("long")
        df_ef["count"] = df_ef["count"].astype("long")
        return df_ef

    def union_track_2_hdfs(self, df_ef, timestack_span):
        """将hdfs中的旧track数据与新的track数据进行合并，进行轨迹合并操作，将合并后的新的轨迹存回hdfs"""
        client = client_init(self.PYHDFS_HOST)
        df_ef["netbar_wacode"] = df_ef["netbar_wacode"].astype(str)
        # ele[mobile_mac, netbar_wacode, time_start, time_end, count]
        # 如果旧轨迹表有数据，则拉过来进行union并在后面进行轨迹合并
        if client.exists(self.HDFS_ST_TRACK_SUB):
            # [mobile_mac, netbar_wacode, time_start, time_end, count]
            df_track = read_hdfs_csv_to_pandas_dataframe(client, self.HDFS_ST_TRACK_SUB)
            df_track["time_start"] = df_track["time_start"].astype("long")
            df_track["time_end"] = df_track["time_end"].astype("long")
            df_track["count"] = df_track["count"].astype("long")
            df_ef = pd.concat([df_ef, df_track], axis=0)
        else:
            client.create(path=self.HDFS_ST_TRACK_SUB, data="")
            sdf_ef = pandas_dataframe_to_string_csv(df_ef)
            client.append(path=self.HDFS_ST_TRACK_SUB, data=sdf_ef)
            return df_ef
        df_ef = df_ef.sort_values("time_start").reset_index().drop(["index"], axis=1)
        df_ef["time_start"] = df_ef["time_start"].astype(str)
        df_ef["time_end"] = df_ef["time_end"].astype(str)
        df_ef["count"] = df_ef["count"].astype(str)
        df_ef["time_on"] = df_ef["time_start"] + ',' + df_ef["time_end"] + ',' + df_ef["count"]
        df_ef = df_ef[["time_on", "mobile_mac", "netbar_wacode"]]
        df_ef["time_on"] = df_ef["time_on"].map(lambda x: "[" + x + "]")
        df_ef = df_ef.groupby(["mobile_mac", "netbar_wacode"])["time_on"].apply(lambda time_on: [','.join(time_on)]).reset_index().rename(columns={"time_on": "time_list"})
        # ele[mobile_mac, netbar_wacode, time_list[time_on(time_start, time_end, count), time_on(time_start, time_end, count), ...]
        df_ef["time_list"] = df_ef.apply(UdfUtils.udf_track_union, axis=1, args=(timestack_span,))
        df_ef["time_list"] = df_ef["time_list"].map(lambda x: str(x.replace(" ", "")))  # .replace("'", "").split("\\],\\["))[1: -1].replace("]", "").replace("[", "")
        df_ef = df_ef.drop(["time_list"], axis=1).join(df_ef["time_list"].str.split("\\],\\[", expand=True).stack().reset_index(level=1, drop=True).rename("time_list"))
        df_ef = df_ef.reset_index()
        df_ef = df_ef.join(df_ef["time_list"].str.split(',', expand=True)).rename(columns={0: "time_start", 1: "time_end", 2: "count"}).drop(["time_list"], axis=1)
        if len(df_ef) == 0: return len(df_ef)
        if len(df_ef) > 0:
            client.delete(path=self.HDFS_ST_TRACK_SUB)
            client.create(path=self.HDFS_ST_TRACK_SUB, data="")
            sdf_ef = pandas_dataframe_to_string_csv(df_ef)
            client.append(path=self.HDFS_ST_TRACK_SUB, data=sdf_ef)
        df_ef = df_ef[["mobile_mac", "netbar_wacode", "time_start", "time_end"]]
        df_track = df_track[["mobile_mac", "netbar_wacode", "time_start", "time_end"]]
        df_track["mobile_mac"] = df_track["mobile_mac"].astype(str)
        df_track["netbar_wacode"] = df_track["netbar_wacode"].astype(str)
        df_track["time_start"] = df_track["time_start"].astype(str)
        df_track["time_end"] = df_track["time_end"].astype(str)
        df_ef = df_ef.append(df_track).append(df_track).drop_duplicates(subset=["mobile_mac", "netbar_wacode", "time_start", "time_end"], keep=False)
        if len(df_ef) == 0: return 0
        return df_ef

    def standardize_etl_data(self, df_track_source):
        """标准化清洗完的数据，并转为pgsql中track表的格式，供后续更新"""
        df_track_source["time_start"] = df_track_source["time_start"].astype(str)
        df_track_source["time_end"] = df_track_source["time_end"].astype(str)
        df_track_source["time_start"] = df_track_source["time_start"].map(lambda x: x + ',0')
        df_track_source["time_end"] = df_track_source["time_end"].map(lambda x: x + ',1')
        df_track_source["probe_time"] = df_track_source["time_start"] + "-" + df_track_source["time_end"]
        df_track_source = df_track_source[["probe_time", "mobile_mac", "netbar_wacode"]].reset_index().drop(["index"], axis=1)
        df_track_source = df_track_source.drop(["probe_time"], axis=1).join(df_track_source["probe_time"].str.split("-", expand=True).stack().reset_index(level=1, drop=True).rename("probe_time"))  # .drop_duplicates()
        df_track_source = df_track_source.reset_index().drop(["index"], axis=1)
        df_track_source = df_track_source.join(df_track_source["probe_time"].str.split(',', expand=True)).drop(["probe_time"], axis=1).rename(columns={0: "probe_time", 1: "flag"})
        df_track_source = df_track_source.rename(columns={"mobile_mac": "probe_data"})
        df_track_source["datasource_table_name"] = "gd_ele_fence"
        df_track_source["flag"] = df_track_source["flag"].map(UdfUtils.udf_null_to_zero)
        print(df_track_source)
        return df_track_source

    def update_track_talbe(self, df_track_output, df_ef_dev):
        """执行更新操作，将需要更新的数据列出，将新的数据追加到track表中"""
        client = pyhdfs.HdfsClient(hosts=self.PYHDFS_HOST, user_name='hdfs')
        if client.exists(self.HDFS_ST_TRACK_INFO_SUB):
            df_track_old = read_hdfs_csv_to_pandas_dataframe(client, self.HDFS_ST_TRACK_INFO_SUB)
            df_track_old = df_track_old[["probe_time", "probe_data", "netbar_wacode", "flag", "datasource_table_name"]]
        else:
            client.create(path=self.HDFS_ST_TRACK_INFO_SUB, data="")
            df_track_old = pd.DataFrame(columns=['probe_time', 'probe_data', 'netbar_wacode', 'flag', 'datasource_table_name'])
        df_track_old = df_track_old[df_track_old["datasource_table_name"] == "gd_ele_fence"]
        df_track_append = df_track_output.append(df_track_old).append(df_track_old).drop_duplicates(keep=False)
        df_track_append["create_time"] = int(time.time() * 1000)
        df_track_append["datasource_id"] = range(len(df_track_append))
        df_track_append["probe_time"] = df_track_append["probe_time"].astype("long").map(lambda x: x * 1000)
        df_ef_dev = df_ef_dev[["netbar_wacode", "device_mac"]]
        df_ef_dev["netbar_wacode"] = df_ef_dev["netbar_wacode"].astype(str)
        df_ef_dev["device_mac"] = df_ef_dev["device_mac"].astype(np.int64)
        df_ef_dev["device_mac"] = df_ef_dev["device_mac"].map(UdfUtils.udf_mac_o2h)
        df_track_append['netbar_wacode'] = df_track_append['netbar_wacode'].astype(str)
        df_track_append = pd.merge(df_track_append, df_ef_dev, on="netbar_wacode", how="left")
        conn_dev, df_dev = read_pgsql_to_pandas_dataframe(self.PGSQL_160, self.PGSQL_214_DEV)
        df_track_append = pd.merge(df_track_append, df_dev[["id", "device_mac"]], how="left", on="device_mac")
        df_track_append = df_track_append.rename(columns={"id": "probe_device_id"})
        df_probe_data = df_track_append[["datasource_id", "probe_data"]]
        df_track_append.drop(["flag", "probe_data"], axis=1)

        if len(df_track_append) == 0: return 0
        if len(df_track_append) > 0:
            client.delete(path=self.HDFS_ST_TRACK_INFO_SUB)
            client.create(path=self.HDFS_ST_TRACK_INFO_SUB, data="")
            sdf_track_append = pandas_dataframe_to_string_csv(df_track_append.append(df_track_old))
            client.append(path=self.HDFS_ST_TRACK_INFO_SUB, data=sdf_track_append)
        df_track_append = df_track_append[["probe_time", "netbar_wacode", "create_time", "datasource_id", "datasource_table_name", "probe_device_id"]]
        df_track_append["datasource_table_name"] = df_track_append["datasource_table_name"].map(lambda x: '\'' + x + '\'')
        print(df_track_append)
        id_list = pg_insert_return_id(self.PGSQL_214, self.PGSQL_214_TRACK, ["probe_time", "netbar_wacode", "create_time", "datasource_id", "datasource_table_name", "probe_device_id"],
                                      pandas_dataframe_to_string_sql_insert_values(df_track_append))
        id_list = pd.DataFrame(id_list, columns=["track_id"])
        df_track_append["track_id"] = id_list["track_id"]
        df_track_append = pd.merge(df_track_append, df_probe_data, on=["datasource_id"], how="left")
        return df_track_append

    def update_attr_record(self, df_track_append, df_attr):
        # df_track_append [netbar_wacode, probe_time, probe_data, flag, datasource_table_name, create_time, datasource_id, probe_device_id]
        # df_track [id, probe_time, netbar_wacode, create_time, datasource_id, datasource_table_name, base_person_id, probe_device_id]
        # 太多，后续无法读取
        df_attr = df_attr[df_attr["attr_type_id"] == 5]
        df_attr = df_attr[["id", "attr_value"]].rename(columns={"attr_value": "probe_data"})
        df_track_append = pd.merge(df_track_append, df_attr, on=["probe_data"], how="left")
        df_track_append = df_track_append.rename(columns={"id": "attr_id"})
        df_track_append["create_time"] = int(time.time() * 1000)
        df_track_append = df_track_append[["track_id", "attr_id", "create_time"]]
        print(df_track_append)
        # pg_insert_return_id(self.PGSQL_214, self.PGSQL_214_ATTR_RECORD, ["track_id", "attr_id", "create_time"],
        #                               pandas_dataframe_to_string_sql_insert_values(df_track_append))
        # append_pgsql(df_track_append, self.PGSQL_214, self.PGSQL_214_ATTR_RECORD)


class UdfUtils(object):
    @staticmethod
    def udf_string_float_to_string(data):
        return data[:data.find('.')]

    @staticmethod
    def udf_rounded_up(df, span):
        """按指定间隔舍入数据"""
        tmp = span / 2
        if df["time_on"] % span > tmp:
            return int(int(df["time_on"] / span) * span + span)
        elif df["time_on"] % span <= tmp:
            return int(int(df["time_on"] / span) * span)

    @staticmethod
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

    @staticmethod
    def udf_null_to_zero(data):
        """输入字符串,为空,返回'0'"""
        if not data:
            return "0"
        else:
            return data

    @staticmethod
    def udf_track_union(time_list, timestack_span):
        time_list = str(time_list["time_list"])[3: -3].split('],[')
        time_list = [x.split(",") for x in time_list]
        time_list = [[int(i) for i in x] for x in time_list]
        time_list = sorted(time_list)
        print(time_list)
        # return time_list
        for i in range(len(time_list) - 1):
            if time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1]:  # 新被旧包含
                time_list[i + 1][0] = time_list[i][0]
                time_list[i + 1][1] = time_list[i][1]
                sum = time_list[i][2] + time_list[i + 1][2]
                time_list[i][2] = sum
                time_list[i + 1][2] = sum
            elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1]:  # 旧被新包含
                time_list[i][0] = time_list[i + 1][0]
                time_list[i][1] = time_list[i + 1][1]
                sum = time_list[i][2] + time_list[i + 1][2]
                time_list[i][2] = sum
                time_list[i + 1][2] = sum
            elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1] and time_list[i][0] - time_list[i + 1][1] <= timestack_span:  # 新在旧左边
                time_list[i][0] = time_list[i + 1][0]
                time_list[i + 1][1] = time_list[i][1]
                sum = time_list[i][2] + time_list[i + 1][2]
                time_list[i][2] = sum
                time_list[i + 1][2] = sum
            elif time_list[i + 1][0] <= time_list[i][0] and time_list[i + 1][1] <= time_list[i][1] and time_list[i][0] - time_list[i + 1][1] > timestack_span:
                pass
            elif time_list[i + 1][0] >= time_list[i][0] and time_list[i + 1][1] >= time_list[i][1] and time_list[i + 1][0] - time_list[i][1] <= timestack_span:  # 新在旧右边
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

    @staticmethod
    def udf_time_etl(time_list, timestack_span):
        """将没用的数据清洗掉，最后生成轨迹，只剩开始时间与结束时间，并对该时间段的被探测次数进行统计，
        其中参数time_list为[[time_on, count],[]]格式，timestack_span为生成新轨迹的规定时间间隔"""
        time_list = str(time_list["time_list"])[3: -3].split('],[')  # .replace('\'', '')
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
    pd.set_option('display.max_columns', None)  # 显示最大列数
    pd.set_option('display.width', None)  # 显示宽度
    pd.set_option('colheader_justify', 'center')  # 显示居中
    pd.set_option('max_colwidth', 200)

    # if len(sys.argv) > 1:
    #     conf_param = json.loads(sys.argv[1])
    #     AnalysisEleFence().etl_real_time(conf_param['file_path'], conf_param['precision_span'], conf_param['timestack_span'])

    AnalysisEleFence().etl_real_time(1, 60, 600)
