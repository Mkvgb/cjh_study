# -*- encoding: utf-8 -*-
"""
@File       :   ax_analysis.py
@Contact    :   ggsddu.com
@Modify Time:   2020/9/4 10:44
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import redis


r = redis.Redis(host='192.168.7.160', port=6379, db=2, password=123456)
# r.set('test:name', 'zhangsan')
print(r.get('test:name'))
r.close()

