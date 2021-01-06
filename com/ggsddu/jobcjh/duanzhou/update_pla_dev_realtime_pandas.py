# -*- encoding: utf-8 -*-
"""
@File       :   st_update_act_pla_dev.py
@Contact    :   ggsddu.com
@Modify Time:   2020/8/27 16:31
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
import pandas as pd


class PGSQLOpr(object):
    def __init__(self, host, port, database, user, password, cursor=None):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.cursor_name = cursor
        self.cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        self.conn.close()

    def update_cursor(self):
        if self.cursor_name:
            self.cursor = self.conn.cursor(name=self.cursor_name)
        else:
            self.cursor = self.conn.cursor()


def create_batch_update_sql(scheme, table, data, field, field_type):
    id_list = list()
    case_str = ""
    for record in data:
        id_list.append(str(record['id']))
        if field_type == "int":
            case_str += f"when {str(record['id'])} then {record[field]}"
        elif field_type == "string":
            case_str += f"when {str(record['id'])} then '{record[field]}'"
    data_range = ','.join(id_list)
    query = f"update {scheme}.{table} set {field} = case id {case_str} end where id in({data_range});"
    return query


def append_pgsql(df, prop_info, tb_info):
    """table='atest' , schema='analysis_etl_gd_ele_fence'"""
    engine = create_engine(
        f"postgresql://{prop_info['user']}:{prop_info['password']}@{prop_info['host']}:{prop_info['port']}/{tb_info['database']}",
        max_overflow=0,  # 超过连接池大小外最多创建的连接
        pool_size=5,  # 连接池大小
        pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
        pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
    )
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


def read_pgsql_to_pandas_dataframe(pginfo, tbinfo):
    conn = f'postgresql+psycopg2://{pginfo["user"]}:{pginfo["password"]}@{pginfo["host"]}:{pginfo["port"]}/{tbinfo["database"]}'
    return conn, pd.concat(pd.read_sql(f'''select * from {tbinfo["schema"]}.{tbinfo["tablename"]}''', con=conn, chunksize=1000))      # chunksize可以实现大文件分块读，且一定得加


class UpdateTimeEleFence(object):
    def __init__(self):
        self.PGSQL_214 = {'host': '192.168.9.214', 'port': 5432, 'user': 'postgres', 'password': 'postgres'}
        self.PGSQL_214_PLA = {'database': 'police_analysis_db', 'schema': 'zhaoqing_duanzhou_db', 'tablename': 'gd_place'}
        self.PGSQL_214_DEV = {'database': 'police_analysis_db', 'schema': 'zhaoqing_duanzhou_db', 'tablename': 'gd_device'}

    def device_place_info_2hdfs_real_time(self, date, table_name):

        """更新场所与设备的update_time，并将更新的数据放到hdfs中，但不会更新设备探测次数，需要调用count_dev_probe_2hdfs进行更新"""
        update_job = UpdateTimeEleFence()
        # df_pla_old[netbar_wacode, longitude, latitude, update_time],每天的更新以及场所设备的更新需要在这一天实时更新之后,这样才不会动场所设备的内容
        # df_dev_old[device_mac,update_time,netbar_wacode,count]
        conn_pla, df_pla_old = read_pgsql_to_pandas_dataframe(self.PGSQL_214, self.PGSQL_214_PLA)
        conn_dev, df_dev_old = read_pgsql_to_pandas_dataframe(self.PGSQL_214, self.PGSQL_214_DEV)

        df_ef = pd.read_csv('D:/code/cjh_study/test.csv')
        df_ef = update_job.prepare_ele_fence(df_ef)
        df_dev_new = update_job.update_time_dev(df_ef, df_dev_old)
        if type(df_dev_new) == int:
            return 0
        update_job.update_time_pla_new(df_dev_new, df_pla_old, df_dev_old)

    def prepare_ele_fence(self, df_ef):
        """实时的数据,并进行数据预处理"""
        df_ef = df_ef.rename(columns={'ap_mac': 'mobile_mac', 'place_code': 'netbar_wacode', 'local_mac': 'device_mac', 'router_mac': 'device_mac', 'collection_equipment_mac': 'device_mac'})
        df_ef = df_ef[["netbar_wacode", "device_mac", "time_on"]]
        df_ef["update_time"] = df_ef["time_on"].map(lambda x: x * 1000)
        df_ef = df_ef.drop(["time_on"], axis=1).where(df_ef.notnull(), None)
        df_ef["device_mac"] = df_ef["device_mac"].map(UdfUtils().udf_null_to_zero)
        df_ef["netbar_wacode"] = df_ef["netbar_wacode"].map(UdfUtils().udf_null_to_zero)
        df_ef = df_ef[df_ef["device_mac"] != "0"][df_ef["netbar_wacode"] != "0"]
        df_ef["device_mac"] = df_ef["device_mac"].map(UdfUtils().udf_mac_o2h)
        # [netbar_wacode, device_mac, update_time]
        return df_ef

    def update_time_dev(self, df_ef, df_dev_old):
        """设备表updatetime更新，具体输出dataframe格式为[id, updatetime]"""
        df_dev_new = df_ef[["device_mac", "update_time"]].groupby("device_mac").agg({"update_time": "max"}).reset_index().rename(columns={"update_time": "update_time_new"})
        df_dev_old["update_time"] = df_dev_old["update_time"].astype("long")
        df_dev_old_2f = df_dev_old[["device_mac", "id"]]
        df_i = pd.merge(df_dev_new["device_mac"], df_dev_old["device_mac"], on=["device_mac"])
        if df_i.count()['device_mac'] == 0:
            return 0
        df_dev_old = pd.merge(df_i, df_dev_old, on="device_mac", how="left")
        df_dev_new = pd.merge(df_i, df_dev_new, how="left", on="device_mac")
        df_dev_new = pd.merge(df_dev_old, df_dev_new, how="outer", on="device_mac")
        df_dev_new["update_time"] = df_dev_new.apply(UdfUtils().udf_select_bigger_filed, axis=1, args=("update_time", "update_time_new"))
        df_dev_new = df_dev_new.drop(["update_time_new"], axis=1)
        df_dev_new = pd.merge(df_dev_new[["device_mac", "update_time"]], df_dev_old_2f, how="left", on="device_mac").drop(["device_mac"], axis=1)
        df_dev_new["update_time"] = df_dev_new["update_time"].map(UdfUtils().udf_null_to_0)
        df_dev_new = df_dev_new[df_dev_new["update_time"] > 0]
        UpdateTimeEleFence().update_table(df_dev_new, self.PGSQL_214_DEV)
        print(df_dev_new)
        return df_dev_new

    def update_time_pla_new(self, df_dev_new, df_pla_old, df_dev_old):
        df_pla_old = df_pla_old[["id", "netbar_wacode", "update_time"]]
        df_dev_old = df_dev_old[["id", "netbar_wacode"]]
        df_dev_new = pd.merge(df_dev_new, df_dev_old, how="left", on=["id"])
        df_pla_new = pd.merge(df_dev_new.rename(columns={"update_time": "update_time_new"}).drop(["id"], axis=1), df_pla_old, how="left", on="netbar_wacode")
        df_pla_new["update_time"] = df_pla_new.apply(UdfUtils().udf_select_bigger_filed, axis=1, args=("update_time", "update_time_new"))
        df_pla_new = df_pla_new.drop(["update_time_new"], axis=1)
        df_pla_new["id"] = df_pla_new["id"].map(UdfUtils().udf_null_to_0)
        df_pla_new = df_pla_new[df_pla_new["id"] > 0].drop(['netbar_wacode'], axis=1)
        print(df_pla_new)
        UpdateTimeEleFence().update_table(df_pla_new, self.PGSQL_214_PLA)

    def update_table(self, df, tb_info):
        if not df.empty:
            t_data = df.to_dict(orient="records")
            prop = self.PGSQL_214
            query = create_batch_update_sql(tb_info["schema"], tb_info["tablename"], t_data, 'update_time', "int")
            with PGSQLOpr(prop["host"], prop["port"], tb_info["database"], prop["user"], prop["password"]) as opr:
                opr.update_cursor()
                opr.cursor.execute(query)
                opr.conn.commit()


class UdfUtils(object):
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

    def udf_select_bigger_filed(self, x, data1, data2):
        """一般为判断两个列元素的大小，并按一定规则留下某一字段的数据"""
        if not x[data1]:
            return x[data2]
        elif not x[data2]:
            return x[data1]
        if x[data1] >= x[data2]:
            return x[data1]
        elif x[data1] < x[data2]:
            return x[data2]

    def udf_null_to_0(self, data):
        if not data:
            data = 0
        return data


if __name__ == '__main__':
    # if len(sys.argv) > 1:
    #     conf_param = json.loads(sys.argv[1])
    #     UpdateTimeEleFence(spark).device_place_info_2hdfs_real_time(conf_param['file_path'], conf_param['table_name'])
    pd.set_option('display.max_columns', None)  # 显示最大列数
    pd.set_option('display.width', None)  # 显示宽度
    pd.set_option('colheader_justify', 'center')  # 显示居中
    pd.set_option('max_colwidth', 200)
    UpdateTimeEleFence().device_place_info_2hdfs_real_time('1', 'co_ele_fence')
