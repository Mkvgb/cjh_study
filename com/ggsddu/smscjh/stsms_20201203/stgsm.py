# -*- encoding: utf-8 -*-
"""
@File       :   stgsm.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/11/24 10:36
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from .smsconf import *
import time
import serial
from .stpdu import *
from loguru import logger

TERMINATOR = '\r'
CTRLZ = '\x1a'


def str2unicode(text):
    code = ''
    for i in text:
        hex_i = hex(ord(i))
        if len(hex_i) == 4:
            code += hex_i.replace('0x', '00')
        else:
            code += hex_i.replace('0x', '')
    return code


def unicode2str(code):
    text = ''
    tmp = ''
    for i in range(len(code)):
        tmp += code[i]
        if len(tmp) == 4:
            text += "\\u" + tmp
            tmp = ''
    text = eval(f'"{text}"')
    return text


def send_sms_close(phone, content):
    """
    phone: 发送的电话号码的数组，电话号码格式是字符串
    content： 发送的内容，也是字符串，可以中文和特殊字符
    """
    s = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ttyUSB0', baudrate=115200, timeout=100)
    s.write(('ATZ' + TERMINATOR).encode())
    atz = str(smsread(s, b'OK'), encoding="utf-8")
    # if atz == 'timeout'
    logger.info('ATZ, return:' + atz)
    s.write(('AT+CMGF=0' + TERMINATOR).encode())
    for i in range(len(phone)):
        # 循环发给所提供的号码
        logger.info("begin phone " + str(i + 1) + " : " + phone[i] + " , content : " + content)
        pdus = encodeSmsSubmitPdu(phone[i], content)
        pdus_len = len(pdus)
        for j in range(pdus_len):
            s.write(('AT+CMGS={0}'.format(pdus[j].tpduLength) + TERMINATOR).encode())  # d30
            logger.info('msg length sent, return:' + smsread(s, b'>'))
            s.write((str(pdus[j]) + CTRLZ).encode())  # d31
            logger.info('msg sent, return:' + smsread(s, b'OK'))
            logger.info("phone " + str(i + 1) + " : part " + str(j + 1) + " done , total : " + str(pdus_len))
    s.close()
    logger.info("serial close!")

def send_sms_unclose(phone, content):
    """
    phone: 发送的电话号码的数组，电话号码格式是字符串
    content： 发送的内容，也是字符串，可以中文和特殊字符
    description: 稳定版本，8.4s一条短信
    """
    s = SERIAL
    for i in range(len(phone)):
        # 循环发给所提供的号码
        logger.info("begin phone " + str(i + 1) + " : " + phone[i] + " , content : " + content)
        pdus = encodeSmsSubmitPdu(phone[i], content)
        pdus_len = len(pdus)
        for j in range(pdus_len):
            s.write(('AT+CMGS={0}'.format(pdus[j].tpduLength) + TERMINATOR).encode())  # d30
            logger.info('msg length sent, return:' + smsread(s, b'>'))
            s.write((str(pdus[j]) + CTRLZ).encode())  # d31
            logger.info('msg sent, return:' + smsread(s, b'OK'))
            logger.info("phone " + str(i + 1) + " : part " + str(j + 1) + " done , total : " + str(pdus_len))
