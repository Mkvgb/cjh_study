# -*- coding:UTF-8 -*-
# from rocketmq.client import Producer, Message
# import json
# import uuid
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf

env_dist = os.environ # environ是在os.py中定义的一个dict environ = {}

print("==============USERPROFILE")
print(env_dist.get('USERPROFILE'))
print("==============end")

print("==============")
print(env_dist.get('PYSPARK_PYTHON'))
print("==============end")

print("==============")
print(env_dist.get('PYSPARK_DRIVER_PYTHON'))
print("==============end")

print(env_dist.get('HOME'))
conf = SparkConf().setAppName('cjh').set("USERPROFILE", "/var/lib/hadoop-hdfs")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.range(10).toJSON().collect()

print(df)

# producer = Producer('policeTracePythonProducer')
# producer.set_namesrv_addr('192.168.9.214:9876')
# producer.start()
# msg = Message("SparkPlan")
# msg.set_body(json.dumps({"name":"sundazhong"}))
# msg.set_tags("ddddddddd")
# producer.send_sync(msg)
# producer.shutdown()

spark.stop()
