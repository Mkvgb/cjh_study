# -*- encoding: utf-8 -*-
"""
@File       :   ax_monitor_size_cjh.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/9/10 14:04
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import time

import pyhdfs
import re
import subprocess
import pandas as pd
import requests


def get_remaining_space_hdfs():
    command = 'hdfs dfsadmin -report'
    cmd = subprocess.Popen(
        command,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        universal_newlines=True,
        shell=True,
        bufsize=1,
        close_fds=True
    )
    line = cmd.stdout.readlines()
    for i in range(len(line)):
        if line[i].find("DFS Remaining") != -1:
            str_size = line[i]
            pattern = re.compile(r'\d+')
            # index = str_size.index('(')
            # remaining_size = str_size[15: index-1]
            remaining_size = int(re.findall(pattern, str_size)[0]) / 1024 / 1024 / 1024
        if line[i].find("DFS Used:") != -1:
            str_size = line[i]
            pattern = re.compile(r'\d+')
            # index = str_size.index('(')
            # remaining_size = str_size[15: index-1]
            used_size = int(re.findall(pattern, str_size)[0]) / 1024 / 1024 / 1024
    remain_percent = used_size / (used_size + remaining_size)
    return remain_percent


def delet_last_file(client, file_path):
    file_path_list = client.listdir(file_path)
    file_path_list = sorted([int(i) for i in file_path_list])
    delet_file = file_path + '/' + str(file_path_list[0])
    print(delet_file)
    client.delete(path=delet_file, recursive=True)


def delet_flag(require_persent):
    used_persent = get_remaining_space_hdfs() * 100
    print(used_persent)
    if used_persent > require_persent:
        return True
    else:
        return False

def client_init(user):
    session = requests.session()
    session.keep_alive = False
    client = pyhdfs.HdfsClient(
        hosts="192.168.7.150:9870",
        user_name=user,
        requests_session=session)
    return client

pd.set_option('display.max_columns', None)  # 显示最大列数
pd.set_option('display.width', None)  # 显示宽度
pd.set_option('colheader_justify', 'center')  # 显示居中
pd.set_option('max_colwidth', 200)
# ax_ele_fence = "/data_sync/gd_ele_fence"
ax_ap_list = "/data_sync/gd_ele_fence"
ax_ele_fence = '/data_sync_test/anxiang/co_ap_list'
client = client_init('hdfs')
csv = client.open("/test/cjh/csv/a")
lt = csv.read().decode().split("\n")
lt = [x.split(",") for x in lt]
df = pd.DataFrame(data=lt[1: -1], columns=lt[0])

print(len(df))

# fs = ','.join(df.columns.values)
# lt = df.values.tolist()
# lt = [','.join(x) for x in lt]
# slt = '\n'.join(lt)
# sdf = fs + '\n' + slt
# print(sdf)



# file_path_list = client.listdir('/data_sync_test/gd_ele_fence')
# print(file_path_list[0])
# print(len(file_path_list))

# client.delete(path='/data_sync_test/gd_ele_fence/202009140800', recursive=True)

# d_flag = delet_flag(require_persent)
#
# while d_flag:
#     delet_last_file(client, ax_ele_fence)
#     # delet_last_file(client, ax_ap_list)
#     d_flag = delet_flag(require_persent)
