# -*- encoding: utf-8 -*-
"""
@File       :   ax_monitor_size_cjh.py    
@Contact    :   suntang.com
@Modify Time:   2020/9/10 14:04
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""

import pyhdfs
import re
import subprocess


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
            remaining_size = re.findall(pattern, str_size)[0]
            print(remaining_size)
    remain_size = int(remaining_size) / 1024 / 1024 / 1024
    return remain_size


def delet_last_file(client, file_path):
    file_path_list = client.listdir(file_path)
    file_path_list = sorted([int(i) for i in file_path_list])
    client.delete(file_path + '/' + str(file_path_list[0]))


def delet_flag(require_size):
    remain_size = get_remaining_space_hdfs()
    if remain_size < require_size:
        return True
    else:
        return False


require_size = 100
ax_ele_fence = "/data_sync/gd_ele_fence"
ax_ap_list = "/data_sync/gd_ele_fence"
client = pyhdfs.HdfsClient(hosts='192.168.7.150:9870', user_name='hdfs')
d_flag = delet_flag(require_size)

while d_flag:
    delet_last_file(client, ax_ele_fence)
    delet_last_file(client, ax_ap_list)
    d_flag = delet_flag(require_size)
