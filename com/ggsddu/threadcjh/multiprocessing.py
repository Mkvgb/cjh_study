# -*- encoding: utf-8 -*-
"""
@File       :   multiprocessing.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/29 17:42
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from multiprocessing import Pool


def test_func(test_arg):
    pass

process_pool = Pool(3)
for test_arg in range(3):
    process_pool.apply_async(test_func, (test_arg,))