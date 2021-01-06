# -*- coding: utf-8 -*-
# @Time : 2020/11/3 15:59
# @Author : XuNanHang
# @File : sms_20201228.py
# @Description :
from __future__ import print_function

import json
from .stgsm import *


def sms_send(deliver_data):
    try:
        logger.info("receive job")
        deliver_data = json.loads(str(deliver_data, encoding="utf-8"))
        cell_list = deliver_data['cells']
        content = deliver_data['content']
        send_sms_unclose(cell_list, content)
    except:
        logger.exception("sms_20201228 consume fail！")


def sms_send_test(ch, method, properties, body):
    try:
        body = json.loads(str(body, encoding="utf-8"))
        logger.info("receive job")
        cell_list = body['cells']
        content = body['content']
        send_sms_unclose(cell_list, content)
    except:
        print("consume fail！")
        # logger.exception(f"{self.pre_msg}:发送短信失败")
        logger.exception(f"ddd:发送短信失败")
    ch.basic_ack(delivery_tag=method.delivery_tag)
