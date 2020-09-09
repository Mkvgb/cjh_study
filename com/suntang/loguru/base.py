# -*- encoding: utf-8 -*-
"""
@File       :   base.py    
@Contact    :   suntang.com
@Modify Time:   2020/9/8 17:23
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
from loguru import logger


@logger.catch
def my_func(a, b):
    return a / b

logger.info("a")


# a = my_func(0 / 0)