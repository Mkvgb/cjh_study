# -*- encoding: utf-8 -*-
"""
@File       :   setting_20210104.py    
@Contact    :   suntang.com
@Modify Time:   2021/1/4 16:34
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
# -*- coding: utf-8 -*-
# @Time : 2020/9/24 17:10
# @Author : XuNanHang
# @File : setting.py
# @Description :
import os

MODE = 'Master'

PROJECT = 'weihai'
DST_DB_HOST = "192.168.1.99"
DST_DB_PORT = '5435'
DST_DB_USER = "postgres"
# DST_DB_PASSWORD = "suntang@123"
DST_DB_PASSWORD = "postgres"
DST_DB_DATABASE = "police_analysis_db"
DST_DB_SCHEMA = "analysis"

RABBIT_MQ_PREFIX = 'dev'
RABBIT_MQ_HOST = '192.168.1.99'
RABBIT_MQ_PORT = '5672'
RABBIT_MQ_USER = 'admin'
RABBIT_MQ_PASSWORD = 'admin'
RABBIT_MQ_VIRTUAL_HOST = '/'
RABBIT_MQ_EXCHANGE = f'{RABBIT_MQ_PREFIX}_direct_exchange'
RABBIT_MQ_SCHEDULE_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_scheduler_router'
RABBIT_MQ_SCHEDULE_QUEUE = f'{RABBIT_MQ_PREFIX}_scheduler_queue'

RABBIT_MQ_PERSON_TRACK_QUEUE = f'{RABBIT_MQ_PREFIX}_track_queue'
RABBIT_MQ_PERSON_TRACK_ROUTE_KEY = f'{RABBIT_MQ_PREFIX}_track_router'

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
REDIS_PASSWORD = 'suntang@police'

HDFS_HOSTS = [
    '192.168.7.150:9870',
    '192.168.7.151:9870',
    '192.168.7.152:9870',
]

SPARK_FILE_PATH = os.path.join(os.path.dirname(__file__), 'sparktask/tasks/')

SMS_PORT = '/dev/ttyUSB0'
SMS_BAUDRATE = 115200
SMS_PIN = '1234'
SMS_QUEUE = 'sms'

FTP_ROOT_DIR = '/var/suntang/data/szgd/'
API_PLACE = 'http://192.168.1.99:8080/police/oauth/place/batch/putOrPost'
API_DEVICE = 'http://192.168.1.99:8080/police/oauth/device/batch/putOrPost'
HDFS_HOST = '192.168.7.150:9870'
HDFS_USER_NAME = 'hdfs'
HDFS_ROOT_DIR = f'/{PROJECT}/src/'

SSH_IP_FILE_SERVER = '192.168.7.152'
SSH_PORT_FILE_SERVER = 22
SSH_USERNAME_FILE_SERVER = 'root'
SSH_PASSWORD_FILE_SERVER = 'suntang@518'

LOCAL_TEMP_DIR='/var/suntang/data/temp/'