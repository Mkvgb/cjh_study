# -*- encoding: utf-8 -*-
"""
@File       :   twiliocjh.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/12 15:30
@Author     :   cjh
@Version    :   1.0
@Desciption :   None
"""
from twilio.rest import Client
# Your Account SID from twilio.com/console
account_sid = "your account sid"
# Your Auth Token from twilio.com/console
auth_token = "your token"
client = Client(account_sid, auth_token)
message = client.messages.create(
    # 这里中国的号码前面需要加86
    to="+15801051609",
    from_="+15801051609",
    body="Hello from Python!")
print(message.sid)
