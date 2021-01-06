# -*- encoding: utf-8 -*-
"""
@File       :   time_change.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/12/16 16:55
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import time
one_day_timestamp = 86400000
def create_time_list():
    start_time = 1607843338000
    end_time = 1608102538000
    day_count = int((end_time - start_time) / one_day_timestamp)
    time_list = []
    for i in range(day_count + 3):
        time_list.append(start_time + (i - 1) * one_day_timestamp)
    day_list = [time.strftime("%Y%m%d", time.localtime(int(i / 1000))) for i in time_list]  # 20200202格式的时间列表

print(int(time.time()*1000))        # 当前世间戳，毫秒级

day = '20201111'
int(time.mktime(time.strptime(day, "%Y%m%d"))) * 1000       # 将日期格式转为时间戳格式
