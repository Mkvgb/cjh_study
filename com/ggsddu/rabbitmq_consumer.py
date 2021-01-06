# -*- coding: utf-8 -*-
# @Time : 2020/9/21 16:36
# @Author : XuNanHang
# @File : rabbitmq_consumer.py
# @Description : 
import pika
import pandas as pd

credentials = pika.PlainCredentials('admin', 'admin')
conn_param = pika.ConnectionParameters(
    '192.168.1.99',
    5672,
    '/',
    credentials)
connection = pika.BlockingConnection(conn_param)
channel = connection.channel()

# # 2、创建一个名为logs的交换机(用于分发日志),模式是发布订阅模式
# channel.exchange_declare(exchange='logs', exchange_type='fanout')

# 3、创建一个随机队列（exclusive=True）
result = channel.queue_declare(queue='test_listening_search_data', exclusive=True,auto_delete=True)
# queue_name = result.method.queue  # 获取随机队列名称
# print('随机队列名: {}'.format(queue_name))

# 4、为名为queue_name的随机队列绑定名为logs的交换机
channel.queue_bind(
    exchange='demo_version',
    queue='test_listening_search_data',
    routing_key='direct_data_center_search',
)

# print('当前MQ发布订阅模式正在等待交换机往消息队列塞消息.......要退出请按 CTRL+C.......')


# 创建回调函数(收到监听队列的消息后执行该回调)
def callback(ch, method, properties, body):
    print("接收到 {} 成功.......".format(body))

    # 给mq发送应答信号，表明数据已经处理完成，可以删除
    # ch.basic_ack(delivery_tag=method.delivery_tag)


# 监听随机队列，一旦有消息出现，则触发回调函数：callback
channel.basic_consume(queue='test_listening_search_data', auto_ack=False, on_message_callback=callback)

# 哪个消费者先处理完谁就去消息队列取
channel.basic_qos(prefetch_count=1)
channel.start_consuming()
