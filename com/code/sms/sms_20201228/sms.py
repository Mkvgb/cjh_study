# -*- coding: utf-8 -*-
# @Time : 2020/11/3 15:59
# @Author : XuNanHang
# @File : sms_20201228.py
# @Description :
from __future__ import print_function
from .stgsm import send_sms_unclose
from loguru import logger


def sms_send(deliver_data):
    try:
        logger.info("receive job")
        # deliver_data = json.loads(str(deliver_data, encoding="utf-8"))
        cell_list = deliver_data['cells']
        content = deliver_data['content']
        send_sms_unclose(cell_list, content)
    except:
        logger.exception("sms_20201228 consume fail！")