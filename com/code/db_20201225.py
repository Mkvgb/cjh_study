from psycopg2.pool import SimpleConnectionPool
import collections
import json
import time
from io import StringIO
import pandas as pd
from psycopg2.pool import SimpleConnectionPool
from sqlalchemy import create_engine
from loguru import logger
DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '6543'
DST_DB_USER = "postgres"
# DST_DB_PASSWORD = "ggsddu@123"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "analysis"
ETL_SCHEMA = 'analysis'


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
            f"select clue_value, start_time, end_time from {ETL_SCHEMA}.warn_clue_rule "
            f"where clue_type = 'mac' "
            f"and (start_time <= now() at time zone 'Asia/Shanghai' and end_time >= now() at time zone 'Asia/Shanghai' ) "
            f"or (start_time = end_time and start_time <= now() at time zone 'Asia/Shanghai' )",
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
                    f"SELECT setval('{ETL_SCHEMA}.attr_id_seq', (SELECT max(id) FROM analysis.attr));")
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

def date2stamp(date):
    return int(time.mktime(time.strptime(str(date), "%Y-%m-%d %H:%m:%S"))) * 1000

db_opr = DBOperator()
warn_clue_df = db_opr.read_clue_rule_data()
warn_clue_df['start_time'] = warn_clue_df['start_time'].map(date2stamp)
print(warn_clue_df)

