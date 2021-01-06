# -*- encoding: utf-8 -*-
"""
@File       :   gsm_cjh_201118.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/11/18 13:41
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from time import sleep
import serial
from .stpdu import *

TERMINATOR = '\r'
CTRLZ = '\x1a'


def send_sms(phone, content, block):
    """
    phone: 发送的电话号码的数组，电话号码格式是字符串
    content： 发送的内容，也是字符串，可以中文和特殊字符
    block： 短信发送间隔，不要小于5s"""
    s = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ttyUSB0', baudrate=115200, timeout=100)
    s.write(('AT+CMGF=0' + TERMINATOR).encode())                                # d18
    for i in range(len(phone)):
        # 循环发给所提供的号码
        pdus = encodeSmsSubmitPdu(phone[i], content)
        for pdu in pdus:
            s.write(('AT+CMGS={0}'.format(pdu.tpduLength) + TERMINATOR).encode())  # d30
            s.write((str(pdu) + CTRLZ).encode())  # d31
            sleep(block)
        sleep(block)


# phone_num = ['15801051609', '15201259733']
# phone_num = ['15801051609']
# content = '☎☏✄☪☣☢☠♨« »큐〓㊚㊛囍㊒㊖☑✔☐☒✘㍿☯☰☷♥♠♤❤♂♀★☆☯✡※卍卐'
# send_sms(phone_num, content, 10)
