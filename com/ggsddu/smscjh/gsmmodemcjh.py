from __future__ import print_function

import logging
import os
from time import sleep

import serial

PORT = '/dev/ttyUSB2'
BAUDRATE = 115200
PIN = '1234'  # SIM card PIN (if any)

from gsmmodem.modem import GsmModem


def send_sms(phone_num, context):
    modem = GsmModem(port=PORT)
    modem.connect(PIN)
    for i in range(len(phone_num)):
        modem.sendSms(phone_num[i], context)
    # modem.smsTextMode = False
    modem.close()


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
phone_num = ['15801051609']
context = '【安乡大数据实战平台】某某人 mac(C8-EE-A6-3F-02-45),于2020-09-20 09:01:00，出现在阳光华庭南门车辆识别进出口gfdgfdgsfdgsfd'
send_sms(phone_num, context)
