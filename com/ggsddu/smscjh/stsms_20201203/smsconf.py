# -*- encoding: utf-8 -*-
"""
@File       :   smsconf.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/12/3 17:45
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
import serial
import subprocess
from loguru import logger
import time
TERMINATOR = '\r'
CTRLZ = '\x1a'


def smsread(ser, stop_flag):
    smsmsg = b''
    t_start = int(time.time())
    while 1:
        if ser.inWaiting():
            smsmsg = smsmsg + ser.read(ser.inWaiting())
            if stop_flag in smsmsg:
                break
            if b'ERROR' in smsmsg:
                return 'return error'
        if int(time.time()) > t_start + 10:
            return 'timeout'
    return str(smsmsg, encoding="utf-8")


result = subprocess.getstatusoutput('ls /dev | grep ttyUSB')
ttyUSB_list = result[1].split('\n')
if len(ttyUSB_list) > 1:
    logger.info("contain " + str(len(ttyUSB_list)) + " ttyUSB, can not recognize!")
    SERIAL = False
elif len(ttyUSB_list) == 0:
    logger.error("ttyUSB no found!")
    SERIAL = False
else:
    SERIAL = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/' + ttyUSB_list[0], baudrate=115200, timeout=100)
    SERIAL.write(('ATZ' + TERMINATOR).encode())
    atz = smsread(SERIAL, b'OK')
    if atz == 'timeout':
        logger.error('请重新拔插modem！')
    logger.info('ATZ, return:' + atz)
    time.sleep(1)
    # SERIAL.write(('AT+CMMS=2' + TERMINATOR).encode())  # 连续发送
    # logger.info('AT+CMMS=2, return:' + smsread(SERIAL, b'OK'))
    # SERIAL.write(('AT+CSQ' + TERMINATOR).encode())  # 文档
    # logger.info('AT+CSQ, return:' + smsread(SERIAL, b'OK'))
    SERIAL.write(('AT+CMGF=0' + TERMINATOR).encode())
    logger.info('AT+CMGF=0, return:' + smsread(SERIAL, b'OK'))
