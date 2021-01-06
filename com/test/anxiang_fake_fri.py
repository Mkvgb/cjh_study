import time
import random
from sqlalchemy import create_engine, Column
from sqlalchemy.orm import sessionmaker
from pandas.core.frame import DataFrame
import pandas as pd
import sys

pd.set_option('display.max_columns', None)  # 显示最大列数
pd.set_option('display.width', None)  # 显示宽度
pd.set_option('colheader_justify', 'center')  # 显示居中
pd.set_option('max_colwidth', 200)


def udf_mac_o2h(data):
    data = data.replace("-", "")
    return int(data, 16)


def pg_insert_return_id(prop_info, tb_info, field_list, insert_values):
    field_list = ','.join(field_list)
    engine = create_engine(
        f"mysql+pymysql://{prop_info['user']}:{prop_info['password']}@{prop_info['host']}:{prop_info['port']}/{tb_info['database']}?charset=utf8",
        max_overflow=0, pool_size=5, pool_timeout=30, pool_recycle=-1)
    dbsession = sessionmaker(bind=engine)
    session = dbsession()
    session.execute(
        f"insert into {tb_info['tablename']} ({field_list}) values {insert_values}")
    session.commit()
    session.close()


def pandas_dataframe_to_string_sql_insert_values(df):
    # fs = ','.join(df.columns.values)
    lt = df.values.tolist()
    lt = [[str(i) for i in x] for x in lt]
    lt = ['(' + (','.join(x)) + ')' for x in lt]
    slt = ','.join(lt)
    return slt


def func_no(smac):
    while 1:
        t = int(time.time())
        t_start = t - 60
        # print(t_start)

        cnt = 1

        # ap_list
        # time_on       ap_mac          str_ap_mac      time_update    local_mac        place_code    channel  encryption_type  time_off
        # mac = 281474959933440
        mac = udf_mac_o2h(smac)
        print(mac)

        time_list = random.sample(range(t_start, t), cnt)

        dfap = DataFrame({"time_on": time_list})
        dfap["ap_mac"] = mac
        dfap["str_ap_mac"] = smac
        dfap["str_ap_mac"] = dfap["str_ap_mac"].map(lambda x: '\'' + x + '\'')
        dfap["time_update"] = dfap["time_on"]
        dfap["route_mac"] = 220927316918565
        dfap["place_code"] = 43072121307948
        dfap["channel"] = 1
        dfap["encryption_type"] = 3
        dfap["time_off"] = 0
        print(dfap)

        PGSQL_214 = {'host': '43.58.5.247', 'port': 3306, 'user': 'root', 'password': 'root123'}
        PGSQL_214_TRACK = {'database': 'police_center_db',
                           'tablename': 'co_ap_list'}  # 'schema': 'zhaoqing_duanzhou_db',
        # pg_insert_return_id(PGSQL_214, PGSQL_214_TRACK,
        #                     ["time_on", "ap_mac", "str_ap_mac", "time_update", "local_mac",
        #                      "place_code", "channel", "encryption_type", "time_off"],
        #                     pandas_dataframe_to_string_sql_insert_values(dfap))

        # ele_fence
        # time_on       ap_mac          str_ap_mac      time_update    route_mac        place_code    signal  time_off

        time_list = random.sample(range(t_start, t), cnt)

        dfele = DataFrame({"time_on": time_list})
        dfele["mobile_mac"] = mac
        dfele["str_mobile_mac"] = smac
        dfele["str_mobile_mac"] = dfele["str_mobile_mac"].map(lambda x: '\'' + x + '\'')
        dfele["time_update"] = dfele["time_on"]
        dfele["router_mac"] = 220927316918565
        dfele["place_code"] = 43072121307948

        s = random.sample(list(range(-80, -30)) * 2, cnt)

        s = [int(i) for i in s]
        dfs = DataFrame({"signal": s})
        dfele = pd.concat([dfele, dfs], axis=1)

        dfele["time_off"] = 0
        print(dfele)

        PGSQL_214 = {'host': '43.58.5.247', 'port': 3306, 'user': 'root', 'password': 'root123'}
        PGSQL_214_TRACK = {'database': 'police_center_db',
                           'tablename': 'co_ele_fence'}  # 'schema': 'zhaoqing_duanzhou_db',
        pg_insert_return_id(PGSQL_214, PGSQL_214_TRACK,
                            ["time_on", "mobile_mac", "str_mobile_mac", "time_update", "router_mac",
                             "place_code", "signal", "time_off"],
                            pandas_dataframe_to_string_sql_insert_values(dfele))
        time.sleep(20)


def func_1():
    while 1:
        t = int(time.time())
        t_start = t - 60
        # print(t_start)

        cnt = 1

        # ap_list
        # time_on       ap_mac          str_ap_mac      time_update    local_mac        place_code    channel  encryption_type  time_off
        # mac = 281474959933440
        smac = ["F0-A3-5A-EF-8A-48", "FC-94-35-55-8f-65", "F0-C4-2F-60-3E-45", "14-A3-2F-FD-6A-14", "F5-6E-35-1A-90-B1", "4E-6F-01-31-FC-F0", "DA-50-2C-C4-5B-BD", "3F-22-17-BE-3E-56", "A9-76-3C-F1-5B-24", "AB-1B-6F-F5-ED-B0", "5F-BA-37-DA-A6-84", "EA-C2-9F-0E-02-32"]
        #smac = ["F5-6E-35-1A-90-B1", "4E-6F-01-31-FC-F0", "DA-50-2C-C4-5B-BD", "3F-22-17-BE-3E-56", "A9-76-3C-F1-5B-24", "AB-1B-6F-F5-ED-B0", "5F-BA-37-DA-A6-84", "EA-C2-9F-0E-02-32"]
        # smac = ["14-A3-2F-FD-6A-14"]

        for i in range(len(smac)):
            mac = udf_mac_o2h(smac[i])
            print(mac)

            time_list = random.sample(range(t_start, t), cnt)

            dfap = DataFrame({"time_on": time_list})
            dfap["ap_mac"] = mac
            dfap["str_ap_mac"] = smac[i]
            dfap["str_ap_mac"] = dfap["str_ap_mac"].map(lambda x: '\'' + x + '\'')
            dfap["time_update"] = dfap["time_on"]
            dfap["route_mac"] = 220927316918565
            dfap["place_code"] = 43072121307948
            dfap["channel"] = 1
            dfap["encryption_type"] = 3
            dfap["time_off"] = 0
            print(dfap)

            PGSQL_214 = {'host': '43.58.5.247', 'port': 3306, 'user': 'root', 'password': 'root123'}
            PGSQL_214_TRACK = {'database': 'police_center_db',
                               'tablename': 'co_ap_list'}  # 'schema': 'zhaoqing_duanzhou_db',
            # pg_insert_return_id(PGSQL_214, PGSQL_214_TRACK,
            #                     ["time_on", "ap_mac", "str_ap_mac", "time_update", "local_mac",
            #                      "place_code", "channel", "encryption_type", "time_off"],
            #                     pandas_dataframe_to_string_sql_insert_values(dfap))

            # ele_fence
            # time_on       ap_mac          str_ap_mac      time_update    route_mac        place_code    signal  time_off

            time_list = random.sample(range(t_start, t), cnt)

            dfele = DataFrame({"time_on": time_list})
            dfele["mobile_mac"] = mac
            dfele["str_mobile_mac"] = smac[i]
            dfele["str_mobile_mac"] = dfele["str_mobile_mac"].map(lambda x: '\'' + x + '\'')
            dfele["time_update"] = dfele["time_on"]
            dfele["router_mac"] = 220927316918565
            dfele["place_code"] = 43072121307948

            s = random.sample(list(range(-80, -30)) * 2, cnt)

            s = [int(i) for i in s]
            dfs = DataFrame({"signal": s})
            dfele = pd.concat([dfele, dfs], axis=1)

            dfele["time_off"] = 0
            print(dfele)

            PGSQL_214 = {'host': '43.58.5.247', 'port': 3306, 'user': 'root', 'password': 'root123'}
            PGSQL_214_TRACK = {'database': 'police_center_db',
                               'tablename': 'co_ele_fence'}  # 'schema': 'zhaoqing_duanzhou_db',
            pg_insert_return_id(PGSQL_214, PGSQL_214_TRACK,
                                ["time_on", "mobile_mac", "str_mobile_mac", "time_update", "router_mac",
                                 "place_code", "signal", "time_off"],
                                pandas_dataframe_to_string_sql_insert_values(dfele))
        time.sleep(20)


smac = sys.argv[1]
if smac == "1":
    func_1()
else:
    func_no(smac)


