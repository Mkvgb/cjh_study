# -*- encoding: utf-8 -*-
"""
@File       :   rabbitmq_20201229.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/29 17:19
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import functools
import json
import time
from multiprocessing import Process
import pika
from importlib import import_module
from pika.exceptions import ConnectionClosed, ChannelClosed
from loguru import logger

RABBIT_MQ_HOST = '192.168.1.99'
RABBIT_MQ_PORT = '5672'
RABBIT_MQ_USER = 'admin'
RABBIT_MQ_PASSWORD = 'admin'
RABBIT_AMQP_URL = f'amqp://{RABBIT_MQ_USER}:{RABBIT_MQ_PASSWORD}@{RABBIT_MQ_HOST}:{RABBIT_MQ_PORT}/'
RABBIT_MQ_VIRTUAL_HOST = '/'
RABBIT_MQ_PREFIX = 'dev'
RABBIT_MQ_EXCHANGE = f'{RABBIT_MQ_PREFIX}_direct_exchange'
RABBIT_MQ_SCHEDULE_QUEUE = f'{RABBIT_MQ_PREFIX}_scheduler_queue'
RABBIT_MQ_SCHEDULE_ROUTER = f'{RABBIT_MQ_PREFIX}_scheduler_router'

RABBIT_MQ_TRACK_QUEUE = f'{RABBIT_MQ_PREFIX}_track_queue'
RABBIT_MQ_TRACK_ROUTER = f'{RABBIT_MQ_PREFIX}_track_router'

RABBIT_MQ_SEARCH_QUEUE = f'{RABBIT_MQ_PREFIX}_search_queue'

class MqUrlCenter(object):
    mq_url = dict()

    def __init__(self):
        self._load_mq_url()

    def get_module(self, msg):
        return self.mq_url.get(msg, None)

    def _load_mq_url(self):
        task_register = import_module('application.mqurls')
        for mq_urls in task_register.mq_urls:
            for url in mq_urls:
                if self.mq_url.get(url['msg'], None):
                    raise ValueError(f"任务[{url['msg']}]重复注册")
                self.mq_url[url['msg']] = url['func']


task_center = MqUrlCenter()


class RabbitMqConsumerBase(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.
    If RabbitMQ closes the connection, this class will stop and indicate
    that reconnection is necessary. You should look at the output, as
    there are limited reasons why the connection may be closed, which
    usually are tied to permission related issues or socket timeouts.
    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.
    """

    def __init__(self, exchange, exchange_type, queue, routing_key, callback_func):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        :param str amqp_url: The AMQP url to connect with
        """
        self.EXCHANGE = exchange
        self.EXCHANGE_TYPE = exchange_type
        self.QUEUE = queue
        self.ROUTING_KEY = routing_key
        self.should_reconnect = False
        self.was_consuming = False
        self.callback = callback_func and callback_func or self.on_message
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        logger.info(f'RabbitMqConsumer->创建连接[{RABBIT_AMQP_URL}]')
        return pika.SelectConnection(
            parameters=pika.URLParameters(RABBIT_AMQP_URL),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            logger.info('RabbitMqConsumer->连接正在关闭/已关闭')
        else:
            logger.info('RabbitMqConsumer->连接正在关闭')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        logger.info('RabbitMqConsumer->连接(Connection)创建成功')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        logger.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning(f'Connection closed, reconnect necessary: {reason}')
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        logger.info('RabbitMqConsumer->开始创建信道(channel)')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        logger.info('RabbitMqConsumer->信道(Channel)创建成功')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        logger.info('RabbitMqConsumer->添加信道(channel)关闭回调方法(callback)')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        logger.warning(f'Channel {channel} was closed: {reason}')
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        logger.info(f'RabbitMqConsumer->开始声明交换机(exchange): {exchange_name}')
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb,
            auto_delete=True,
            durable=True
        )

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        logger.info(f'RabbitMqConsumer->交换机(exchange)创建成功: {userdata}')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        logger.info(f'RabbitMqConsumer->开始声明队列(queue): {queue_name}')
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(
            queue=queue_name,
            callback=cb,
            durable=True,
            auto_delete=True,
            # exclusive=True
        )

    def on_queue_declareok(self, _unused_frame, userdata):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        queue_name = userdata
        logger.info(f'RabbitMqConsumer->绑定交换机:[{self.EXCHANGE}]至队列[{queue_name}],routing_key为:{self.ROUTING_KEY}')
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will set the prefetch count for the channel.
        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
        :param str|unicode userdata: Extra user data (queue name)
        """
        logger.info(f'RabbitMqConsumer->队列[{userdata}]绑定成功')
        self.set_qos()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        logger.info(f'RabbitMqConsumer->QOS数值设置为: {self._prefetch_count}')
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        logger.info('RabbitMqConsumer->开始启动消费者(执行相关RPC命令)')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE,
            self.callback
        )
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        logger.info('RabbitMqConsumer->添加消费者(consumer)撤销回调方法(callback)')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        logger.info(f'RabbitMqConsumer->Consumer was cancelled remotely, shutting down: {method_frame}')
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body
        """
        logger.info(f'RabbitMqConsumer->接受到消息 # {basic_deliver.delivery_tag} from {properties.app_id}')
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.
        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        logger.info(f'RabbitMqConsumer->确认消息 {delivery_tag}')
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            logger.info('RabbitMqConsumer->正在发送取消RPC指令到RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)
        """
        self._consuming = False
        logger.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        logger.info('RabbitMqConsumer->正在关闭信道(channel)')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            logger.info('RabbitMqConsumer->关闭消费者(consumer)中')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            logger.info('RabbitMqConsumer->消费者(consumer)关闭成功')


class RabbitMqConsumer(Process):
    """This is an example consumer that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.
    """

    def __init__(self, exchange, exchange_type, queue, routing_key, callback_func=None):
        self._reconnect_delay = 0
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_key = routing_key
        self.callback_func = callback_func and callback_func or self.analysis_callback_func
        self._consumer = RabbitMqConsumerBase(
            exchange,
            exchange_type,
            queue,
            routing_key,
            self.callback_func
        )
        Process.__init__(self)

    def run(self):
        while True:
            try:
                logger.info(f'RabbitMqConsumer->开始监听')
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            logger.info(f'RabbitMqConsumer->{reconnect_delay}秒后重新连接')
            time.sleep(reconnect_delay)
            self._consumer = RabbitMqConsumerBase(
                self.exchange,
                self.exchange_type,
                self.queue,
                self.routing_key,
                self.callback_func
            )

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay

    def analysis_callback_func(self, _unused_channel, basic_deliver, properties, body):
        """
        mq消息回调
        """
        logger.info(f'RabbitMqConsumer->收到消息，开始消费')
        load_data = json.loads(body)

        # load_data = pickle.loads(zlib.decompress(body))
        msg_list = load_data.get('msg_list', None)
        deliver_data = load_data.get('deliver_data', None)

        if not msg_list or not deliver_data:
            logger.warning(f'RabbitMqConsumer->消息缺失，消费失败，已忽略该消息')
        else:
            for msg in msg_list:
                task = task_center.get_module(msg)
                if not task:
                    logger.warning(f"RabbitMqConsumer->未找到任务[{msg}]")
                    break
                try:
                    task(deliver_data)
                except Exception as e:
                    logger.exception(f'RabbitMqConsumer->模块({msg})调用异常,错误为{e}')
        _unused_channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)


class RabbitMQServer(object):
    def __init__(self, exchange=None, route_key=None, queue=None, exchange_type=None):
        self.connection = None
        self.channel = None
        self.exchange = exchange if exchange else RABBIT_MQ_EXCHANGE
        self.route_key = route_key if route_key else RABBIT_MQ_SCHEDULE_ROUTER
        self.queue = queue if queue else RABBIT_MQ_SCHEDULE_QUEUE
        self.exchange_type = exchange_type if exchange_type else 'direct'

    def init_connect(self):
        try:
            self.close_connect()
            credentials = pika.PlainCredentials(RABBIT_MQ_USER, RABBIT_MQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBIT_MQ_HOST,
                port=int(RABBIT_MQ_PORT),
                virtual_host=RABBIT_MQ_VIRTUAL_HOST,
                credentials=credentials,
                heartbeat=0
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
        except Exception as e:
            logger.exception(e)

    def init_exchange(self):
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=self.exchange_type,
            durable=True,
            auto_delete=True
        )
        try:
            self.channel.queue_declare(queue=self.queue, auto_delete=True, durable=True)
            self.channel.queue_bind(self.queue, self.exchange, routing_key=self.route_key)
        except pika.exceptions.ChannelClosedByBroker as e:
            logger.warning(f"MqPublish 声明队列失败--队列名{self.queue}，错误信息[{e}]")
            self.init_connect()

    def close_connect(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()


class RabbitPublisher(RabbitMQServer):
    def __init__(self, exchange, route_key, queue):
        super(RabbitPublisher, self).__init__(exchange, route_key, queue)
        self.init_connect()
        self.init_exchange()

    def publish(self, message):
        try:
            properties = pika.BasicProperties(
                delivery_mode=2,
                content_encoding="UTF-8",
                content_type="text/plain",
            )
            self.channel.basic_publish(exchange=self.exchange, routing_key=self.route_key, body=message,
                                       properties=properties)
            self.close_connect()
        except (ConnectionClosed, ChannelClosed):
            time.sleep(1)
            self.init_connect()
        except Exception as e:
            logger.exception(f"RabbitPublisher发生错误->{e}")
            time.sleep(1)
            self.init_connect()


consumer = RabbitMqConsumer(
            RABBIT_MQ_EXCHANGE,
            'direct',
            RABBIT_MQ_SCHEDULE_QUEUE,
            RABBIT_MQ_SCHEDULE_ROUTER
        )
consumer.start()        # 消费者启动，不用建交换机队列

# 生产这推送
publisher = RabbitPublisher(exchange=RABBIT_MQ_EXCHANGE,
                            route_key=RABBIT_MQ_TRACK_ROUTER,
                            queue=RABBIT_MQ_TRACK_QUEUE)
publisher.publish(json.dumps({'a': 1}))
