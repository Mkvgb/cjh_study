"""
@File       :   testmq.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/27 18:35
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
# -*- encoding: utf-8 -*-
import serial
import time
import pika
from pika.exceptions import ConnectionClosed, ChannelClosed
from loguru import logger
from com.ggsddu.smscjh.stsms_20201203.sms import sms_send_test
import sys


class RabbitMQServer(object):
    def __init__(self, exchange=None, route_key=None, queue=None):
        self.connection = None
        self.channel = None
        self.exchange = exchange if exchange else "server_mq"
        self.route_key = route_key if route_key else "schedule"
        self.queue = queue if queue else "direct_scheduler"

    def init_connect(self):
        try:
            self.close_connect()
            credentials = pika.PlainCredentials("admin", "admin")
            parameters = pika.ConnectionParameters(
                host="192.168.1.99",
                port=5672,
                virtual_host="/",
                credentials=credentials,
                heartbeat=0
            )
            self.connection = pika.BlockingConnection(parameters)

            self.channel = self.connection.channel()

            if isinstance(self, RabbitConsumer):
                self.channel.basic_consume(
                    queue=self.queue,
                    on_message_callback=self.callback_func,
                    auto_ack=False
                )
        except Exception as e:
            logger.exception(e)

    def close_connect(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()


class RabbitConsumer(RabbitMQServer):
    def __init__(self, callback_func, queue=None):
        super(RabbitConsumer, self).__init__(queue=queue)
        self.callback_func = callback_func

    def start_consumer(self):
        while True:
            try:
                self.init_connect()
                self.channel.start_consuming()
            except (ConnectionClosed, ChannelClosed):
                time.sleep(1)
                self.init_connect()
            except Exception as e:
                time.sleep(1)
                logger.exception(f"RabbitConsumer发生错误->{e}")
                self.init_connect()


class RabbitPublisher(RabbitMQServer):
    def __init__(self, exchange=None, route_key=None):
        super(RabbitPublisher, self).__init__(exchange, route_key)
        self.init_connect()

    def publish(self, message):
        try:
            properties = pika.BasicProperties(
                delivery_mode=2,
                content_encoding="UTF-8",
                content_type="text/plain",
            )
            self.channel.basic_publish(exchange=self.exchange, routing_key=self.route_key, body=message,
                                       properties=properties)
        except (ConnectionClosed, ChannelClosed):
            time.sleep(1)
            self.init_connect()
        except Exception as e:
            logger.exception(f"RabbitPublisher发生错误->{e}")
            time.sleep(1)
            self.init_connect()


def consumer_test(ch, method, properties, body):
    try:
        # cell_list = body['cells']
        # context = body['context']
        print(body)
    except:
        print("consume fail！")
        # logger.exception(f"{self.pre_msg}:发送短信失败")
    time.sleep(10)
    ch.basic_ack(delivery_tag=method.delivery_tag)


publisher = RabbitPublisher(
    exchange="weihai_direct_exchange",
    route_key="schedule_router"
)
# phone = sys.argv[1]
# cnt = sys.argv[2]
for i in range(10):
    # publisher.publish('{"cells": ["15801051609"], "content": "' + str(i+1) + ' -- 某某人与反馈疗法三级独立声卡的房间里看到到家乐福咖啡家里的事开发建设狄拉克附件弗兰克大家风范了巨大十分激烈某某人与反馈疗法三级独立声卡的房间里看到到家乐福咖啡家里的事开发建设狄拉克附件弗兰克"}')  # 传字符串即可
    # publisher.publish('{"cells": ["15801051609"], "content": "' + str(i) + '"}')  # 传字符串即可
    # publisher.publish('{"cells": ["' + phone + '"], "content": "【安乡大数据实战平台】某某人 mac(' + str(i+1) + '),于2020-xx-xx xx:xx:xx，出现在2组—潺陵路农商行"}')  # 传字符串即可
    publisher.publish(('{"msg_list": ["externals.sms_send",],"deliver_data": {"cells": ["15801051609", "15801051609"], "content": "某某人"}}'))
consumer = RabbitConsumer(sms_send_test, "weihai_scheduler_queue")
consumer.start_consumer()


def smsread(ser, stop_flag):
    smsmsg = b''
    t_start = int(time.time())
    while 1:
        if ser.inWaiting():
            smsmsg = smsmsg + ser.read(ser.inWaiting())
            if stop_flag in smsmsg:
                break
            if b'ERROR' in smsmsg:
                return 'return error'
        if int(time.time()) > t_start + 10:
            return 'timeout'
    return str(smsmsg, encoding="utf-8")
#
# TERMINATOR = '\r'
# CTRLZ = '\x1a'
# s = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ttyUSB0', baudrate=115200, timeout=100)
# s.write(TERMINATOR.encode())
# s.write(CTRLZ.encode())
# s.write(('AT'+TERMINATOR).encode())
# print(s.read(64))
# s.write(TERMINATOR.encode())
# s.write(CTRLZ.encode())
#
# s.write(('AT'+TERMINATOR).encode())
# print(smsread(s, b'OK'))
# # s.write(TERMINATOR.encode())
# # s.write(CTRLZ.encode())



# TERMINATOR = '\r'
# CTRLZ = '\x1a'
# s = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ttyUSB0', baudrate=115200, timeout=100)
# s.write(CTRLZ.encode())
# s.write(TERMINATOR.encode())
# s.write(('AT'+TERMINATOR).encode())
# print(smsread(s, b'OK'))
# # s.write(TERMINATOR.encode())
# # s.write(CTRLZ.encode())
# time.sleep(0.2)
# s.sendBreak(duration=0.02)
# time.sleep(0.2)
# s.close()