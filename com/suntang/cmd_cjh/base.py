# -*- encoding: utf-8 -*-
"""
@File       :   base.py    
@Contact    :   suntang.com
@Modify Time:   2020/9/10 15:58
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import subprocess

command = 'hdfs dfsadmin -report'
cmd = subprocess.Popen(
    command,
    stderr=subprocess.STDOUT,
    stdout=subprocess.PIPE,
    universal_newlines=True,
    shell=True,
    bufsize=1
)
line = cmd.stdout.readlines()
for i in range(len(line)):
    if line[i].find("DFS Remaining") != -1:
        str_size = line[i]
        index = str_size.index('(')
        remaining_size = str_size[15: index-1]
remain_size = int(remaining_size) / 1024 / 1024 / 1024
print(remain_size)

# s = '123456'
# print(s.index('4'))
# print(s.find('67'))