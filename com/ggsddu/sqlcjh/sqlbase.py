# -*- encoding: utf-8 -*-
"""
@File       :   sqlbase.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/30 9:07
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
import pymysql
from pymysql.cursors import SSDictCursor, DictCursor
import pandas as pd

class PgSQLManage(object):
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def init_conn(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            cursor_factory=RealDictCursor,
            keepalives=2,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )

    def create_engine(self):
        from sqlalchemy import create_engine
        engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}",
            max_overflow=0,  # 超过连接池大小外最多创建的连接
            pool_size=2,  # 连接池大小
            pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
            pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
        )
        return engine

    def get_conn_string(self):
        return f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'

    def init_cursor(self, is_server_side_cursor=False):
        if is_server_side_cursor:
            self.cursor = self.conn.cursor(name=str(uuid.uuid1()))
        else:
            self.cursor = self.conn.cursor()

    def close_conn(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class MySQLManage(object):
    def __init__(self, host, port, user, password, database):
        self.conn = None
        self.cursor = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def init_conn(self):
        self.close_conn()
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8'
        )

    def init_cursor(self, is_server_side_cursor=False):
        if is_server_side_cursor:
            self.cursor = self.conn.cursor(cursor=SSDictCursor)
        else:
            self.cursor = self.conn.cursor(cursor=DictCursor)

    def close_conn(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None


class SQLManageFactory(object):

    db_manage_map = {
        "pgsql": PgSQLManage,
        "mysql": MySQLManage
    }

    def __init__(self, host=None, port=None, user=None, password=None, database=None, db_type='pgsql'):
        self.sql_manage = self.db_manage_map[db_type](
            host=host and host or DST_DB_HOST,
            port=port and port or DST_DB_PORT,
            user=user and user or DST_DB_USER,
            password=password and password or DST_DB_PASSWORD,
            database=database and database or DST_DB_DATABASE
        )

    def get_manage(self):
        return self.sql_manage

DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '5435'
DST_DB_USER = "postgres"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "analysis"
db_manage = SQLManageFactory(
            host=DST_DB_HOST,
            port=int(DST_DB_PORT),
            user=DST_DB_USER,
            password=DST_DB_PASSWORD,
            database=DST_DB_DATABASE,
            db_type='pgsql'
        ).get_manage()
db_manage.init_conn()
db_manage.init_cursor()     # is_server_side_cursor=True
query = f'select * from {DST_DB_SCHEMA}.attr'
db_manage.cursor.execute(query)

pd.read_sql(query, db_manage.conn)

db_manage.close_conn()
