# -*- encoding: utf-8 -*-
"""
@File       :   stgsm.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/11/24 10:36
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
from application.externals.sms.smsconf import smsread, SERIAL, TERMINATOR, CTRLZ, serial_init
from application.externals.sms.stpdu import encodeSmsSubmitPdu
from loguru import logger


def send_sms_unclose(phone, content):
    """
    phone: 发送的电话号码的数组，电话号码格式是字符串
    content： 发送的内容，也是字符串，可以中文和特殊字符
    description: 稳定版本，4s一条短信
    """
    s = SERIAL
    for i in range(len(phone)):
        # 循环发给所提供的号码
        logger.info("begin phone " + str(i + 1) + " : " + phone[i] + " , content : " + content)
        pdus = encodeSmsSubmitPdu(phone[i], content)
        pdus_len = len(pdus)
        for j in range(pdus_len):
            while 1:
                s.write(('AT+CMGS={0}'.format(pdus[j].tpduLength) + TERMINATOR).encode())  # d30
                result = smsread(s, b'>')
                if result == 'timeout' or result == 'fail':
                    s = serial_init()
                    logger.info('modem disconnect! redoing')
                    continue
                else:
                    logger.info('msg length sent, return:' + result)
                s.write((str(pdus[j]) + CTRLZ).encode())  # d31
                result = smsread(s, b'OK')
                if result == 'timeout' or result == 'fail':
                    s = serial_init()
                    logger.info('modem disconnect! redoing')
                    continue
                else:
                    logger.info('msg sent, return:' + result)
                    logger.info("phone " + str(i + 1) + " : part " + str(j + 1) + " done , total : " + str(pdus_len))
                break


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