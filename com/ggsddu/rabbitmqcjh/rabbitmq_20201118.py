# -*- encoding: utf-8 -*-
"""
@File       :   rabbitmq_20201118.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/18 15:33
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import time
import pika
from pika.exceptions import ConnectionClosed, ChannelClosed
from loguru import logger


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
    exchange="cjh_ex_test",
    route_key="cjh_test"
)
publisher.publish("a")  # 传字符串即可

consumer = RabbitConsumer(consumer_test, "cjh_que_test")
consumer.start_consumer()
