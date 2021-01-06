# -*- encoding: utf-8 -*-
"""
@File       :   ax_analysis.py
@Contact    :   ggsddu.com
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
class ClassTest():
    def func_test(self):
        from loguru import logger
        # format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module} | {message}"
        logger.add(f'D:/code/cjh_study/com/test/log.test', format="{time:YYYY-MM-DD HH:mm:ss}| {level} | {module}:{function}:{line} | {message}", retention="10 days",
                   encoding='utf-8', level='INFO')
        # format的内容去LOGURU_FORMAT找，双击shift
        logger.info('log test')
ClassTest().func_test()