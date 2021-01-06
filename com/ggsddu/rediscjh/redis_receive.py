# -*- encoding: utf-8 -*-
"""
@File       :   redis_receive.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/9/4 13:41
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
# from com.ggsddu.rediscjh.redis_help_cjh import RedisHelper
import redis

class RedisHelper:
    def __init__(self):
        self.__conn = redis.Redis(host='192.168.7.160', port=6379, db=3, password=123456)
        self.chan_sub = 'fm104.5'
        self.chan_pub = 'fm104.5'

    def public(self, msg):
        self.__conn.publish(self.chan_pub, msg)
        return True

    def subscribe(self):
        pub = self.__conn.pubsub()
        pub.subscribe(self.chan_sub)
        pub.parse_response()
        return pub

obj = RedisHelper()
redis_sub = obj.subscribe()

while True:
    msg = redis_sub.parse_response()
    print(msg)
