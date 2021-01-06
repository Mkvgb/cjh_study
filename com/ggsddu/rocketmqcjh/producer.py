# -*- encoding: utf-8 -*-
"""
@File       :   producer.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/9/11 10:37
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from rocketmq.client import Producer, Message
import getpass


env_dist = os.environ

print("HOME:", env_dist.get('HOME'))
os.environ['HOME'] = "/var/log"
conf = SparkConf().setAppName('cjh')
print("HOME:", env_dist.get('HOME'))
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.range(10).show()
producer = Producer('PID-123')
producer.set_name_server_address('192.168.9.214:9876')
producer.start()

msg = Message('YOUR-TOPIC')
msg.set_keys('XXX')
msg.set_tags('test')
msg.set_body('msg coming')
ret = producer.send_sync(msg)
print(ret.status, ret.msg_id, ret.offset)
producer.shutdown()
spark.stop()
