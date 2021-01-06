# -*- encoding: utf-8 -*-
"""
@File       :   ax_analysis.py
@Contact    :   ggsddu.com
@Modify Time:   2020/8/28 14:58
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
import paramiko
import json

hostname = "192.168.9.201"
port = 22
username = "root"
password = "cq2002"
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=hostname, port=port, username=username, password=password, compress=True)
sftp_client = client.open_sftp()
remote_file = sftp_client.open("/home/test/cjh/20200721/00/30/20200721003301100_145_441200_723005104_008.log")
try:
    for line in remote_file:
        print(line)
finally:
    remote_file.close()
json.load(remote_file)