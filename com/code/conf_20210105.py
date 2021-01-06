# -*- coding: utf-8 -*-
# @Time : 2020/9/24 17:10
# @Author : XuNanHang
# @File : setting.py
# @Description :
import os
# pip install nacos-sdk-python
import nacos
import yaml
from urllib.parse import urlparse


def nacos_conf_quote(conf, key_list):
    for i in range(len(key_list)):
        if conf.get(key_list[i]):
            conf = conf.get(key_list[i])
    return conf


nacos_addr = "192.168.1.99:8848"
namespace = 'dev'
client = nacos.NacosClient(nacos_addr, namespace=namespace, username="nacos", password="suntang@123")
data_id = "application-dev.yml"
group = "dev"
conf_nacos = client.get_config(data_id, group)
conf_nacos = yaml.load(conf_nacos)

MODE = conf_nacos.get('mode') or 'Master'
PROJECT = conf_nacos.get('project') or 'weihai'

"""pgsql conf"""
conf = nacos_conf_quote(conf_nacos, ['spring', 'datasource'])
parsed = urlparse(conf.get('url'))
parsed = urlparse(parsed.path)
DST_DB_HOST = parsed.hostname or "192.168.1.99"
DST_DB_PORT = parsed.port or '6543'
DST_DB_DATABASE = parsed.path.replace('/', '') or "police_analysis_db"
DST_DB_USER = conf.get('username') or "postgres"
DST_DB_PASSWORD = conf.get('password') or "postgres"
DST_DB_SCHEMA = conf.get('schema') or "analysis"

"""rabbitmq conf"""
conf = nacos_conf_quote(conf_nacos, ['spring', 'rabbitmq'])
RABBIT_MQ_PREFIX = conf.get('prefix') or 'dev'
RABBIT_MQ_HOST = conf.get('host') or '192.168.1.99'
RABBIT_MQ_PORT = conf.get('port') or '5672'
RABBIT_MQ_USER = conf.get('username') or 'admin'
RABBIT_MQ_PASSWORD = conf.get('password') or 'admin'
RABBIT_MQ_VIRTUAL_HOST = '/'

conf = nacos_conf_quote(conf_nacos, ['exchange'])
RABBIT_MQ_EXCHANGE = conf.get('direct') or f'{RABBIT_MQ_PREFIX}_direct_exchange'

conf = nacos_conf_quote(conf_nacos, ['queue'])
RABBIT_MQ_SCHEDULE_QUEUE = conf.get('sms') or f'{RABBIT_MQ_PREFIX}_scheduler_queue'
RABBIT_MQ_PERSON_TRACK_QUEUE = conf.get('track') or f'{RABBIT_MQ_PREFIX}_track_queue'
RABBIT_MQ_DATA_CACHE_QUEUE = conf.get('cache') or f'{RABBIT_MQ_PREFIX}_cache_queue'
RABBIT_MQ_SEARCH_QUEUE = conf.get('search') or f'{RABBIT_MQ_PREFIX}_search_queue'

conf = nacos_conf_quote(conf_nacos, ['router'])
RABBIT_MQ_SCHEDULE_ROUTE_KEY = conf.get('warnSms') or f'{RABBIT_MQ_PREFIX}_scheduler_router'
RABBIT_MQ_PERSON_TRACK_ROUTE_KEY = conf.get('personTrack') or f'{RABBIT_MQ_PREFIX}_track_router'
RABBIT_MQ_DATA_CACHE_ROUTE_KEY = conf.get('cache') or f'{RABBIT_MQ_PREFIX}_cache_router'
RABBIT_MQ_SEARCH_ROUTE_KEY = conf.get('search') or f'{RABBIT_MQ_PREFIX}_search_router'

RABBIT_AMQP_URL = f'amqp://{RABBIT_MQ_USER}:{RABBIT_MQ_PASSWORD}@{RABBIT_MQ_HOST}:{RABBIT_MQ_PORT}/'
if not RABBIT_MQ_VIRTUAL_HOST == '/':
    RABBIT_AMQP_URL += RABBIT_MQ_VIRTUAL_HOST

"""redis conf"""
conf = nacos_conf_quote(conf_nacos, ['spring', 'redis'])
REDIS_HOST = conf.get('host') or '192.168.1.99'
REDIS_DB = conf.get('database') or 1
REDIS_PORT = conf.get('port') or '6379'
REDIS_PASSWORD = conf.get('password') or 'suntang@police'

"""cdh conf"""
conf = nacos_conf_quote(conf_nacos, ['cdh'])
HDFS_HOSTS = conf.get('hosts') or ['192.168.7.150:9870', '192.168.7.151:9870', '192.168.7.152:9870']
HDFS_HOST = conf.get('host') or '192.168.7.150:9870'
HDFS_USER_NAME = conf.get('hdfs_user_name') or 'hdfs'
HDFS_ROOT_DIR = f'/{PROJECT}{conf.get("hdfs_root_dir")}' or f'/{PROJECT}/src/'

SPARK_FILE_PATH = os.path.join(os.path.dirname(__file__), 'sparktask/tasks/')

"""file_sync conf"""
conf = nacos_conf_quote(conf_nacos, ['file_sync'])
FTP_ROOT_DIR = conf.get('ftp_root_dir') or '/var/suntang/data/szgd/'
API_PLACE = conf.get('api_place') or 'http://192.168.1.99:8080/police/oauth/place/batch/putOrPost'
API_DEVICE = conf.get('api_device') or 'http://192.168.1.99:8080/police/oauth/device/batch/putOrPost'
LOCAL_TEMP_DIR = conf.get('local_tmp_dir') or '/var/suntang/data/temp/'

conf = nacos_conf_quote(conf_nacos, ['ssh_server'])
SSH_IP_FILE_SERVER = conf.get('ssh_ip_file_server') or '192.168.7.152'
SSH_PORT_FILE_SERVER = conf.get('ssh_port_file_server') or 22
SSH_USERNAME_FILE_SERVER = conf.get('ssh_username_file_server') or 'root'
SSH_PASSWORD_FILE_SERVER = conf.get('ssh_password_file_Server') or 'suntang@518'
