# -*- encoding: utf-8 -*-
"""
@File       :   urlfilter.py    
@Contact    :   suntang.com
@Modify Time:   2021/1/4 16:00
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from urllib.parse import urlparse
url = 'jdbc:postgresql://192.168.1.99:5435/police_analysis_db?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8'
parsed = urlparse(url)
parsed = urlparse(parsed.path)
print(parsed)
print(parsed.hostname)
print(parsed.port)
print(parsed.path)