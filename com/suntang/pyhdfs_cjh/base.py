# -*- encoding: utf-8 -*-
"""
@File       :   base.py    
@Contact    :   suntang.com
@Modify Time:   2020/9/10 14:04
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
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
    if remain_size < require_size: return True
    else: return False


require_size = 100
ax_ele_fence = "/data_sync/gd_ele_fence"
ax_ap_list = "/data_sync/gd_ele_fence"
client = pyhdfs.HdfsClient(hosts='192.168.7.150:9870', user_name='hdfs')
d_flag = delet_flag(require_size)

while d_flag > 0:
    delet_last_file(client, ax_ele_fence)
    delet_last_file(client, ax_ap_list)
    d_flag = delet_flag(require_size)



# des_dir = "C:/Users/DELL/Desktop/"
#
# # print(csv_list)
# # 文件总数
# fileCount = client.get_content_summary(ax_ele_fence).fileCount
# # print(fileCount)
#
# # a = client.get_content_summary("/data_sync/co_ap_list/202007210800").length / 1024 / 1024
# # a = client.get_content_summary("/data_sync/co_ele_fence/202007210800").length / 1024 / 1024
#
# a = client.get_content_summary("/").length / 1024 / 1024
#
# # print(a)
#
# # if len(csv_list) != 0:
# #     m_len = 0
# #     m_file = csv_list[0]
# #     part_count = 1
# #     for i in range(len(csv_list) - 1):
# #         f_len = client.get_content_summary(test_dir + csv_list[i + 1]).length / 1024 / 1024
# #         m_len += f_len
# #         if m_len < 115:
# #             client.concat(test_dir + m_file, [test_dir + csv_list[i + 1]])
# #         else:
# #             part = "part_" + str(part_count) + ".csv"
# #             client.rename(test_dir + m_file, test_dir + part)
# #             client.copy_to_local(test_dir + part, des_dir + part)
# #             client.delete(test_dir + part)
# #             client.copy_from_local(des_dir + part, test_dir + part)
# #             m_len = f_len
# #             m_file = csv_list[i + 1]
# #             int(part_count)
# #             part_count += 1
# #     part = "part_" + str(part_count) + ".csv"
# #     client.rename(test_dir + m_file, test_dir + part)
# #     client.copy_to_local(test_dir + part, des_dir + part)
# #     client.delete(test_dir + part)
# #     client.copy_from_local(des_dir + part, test_dir + part)
