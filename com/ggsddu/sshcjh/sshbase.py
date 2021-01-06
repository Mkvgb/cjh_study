# -*- encoding: utf-8 -*-
"""
@File       :   sshbase.py    
@Contact    :   suntang.com
@Modify Time:   2020/12/29 18:00
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import paramiko
import pandas as pd
SSH_HOST = '192.168.7.152'
SSH_PORT = 22
SSH_USERNAME = 'root'
SSH_PASSWORD = 'suntang@518'


def get_ssh_client():
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(SSH_HOST, SSH_PORT, SSH_USERNAME, SSH_PASSWORD, timeout=5)
    return ssh_client


ssh_client = get_ssh_client()
sftp_client = ssh_client.open_sftp()

file = '/home/test.file'
with sftp_client.open(file, 'r') as f:      # 确保不异常退出，保证执行close
    df = pd.read_json(f, encoding='utf-8', orient='records')
    sftp_client.rename(file, file + 'bak')

sftp_client.close()
ssh_client.close()
