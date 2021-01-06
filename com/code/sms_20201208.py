"""
@File       :   testmq.py
@Contact    :   ggsddu.com
@Modify Time:   2020/11/27 18:35
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
# -*- encoding: utf-8 -*-
import pika
from pika.exceptions import ConnectionClosed, ChannelClosed
import sys
import json
from time import sleep
TERMINATOR = '\r'
CTRLZ = '\x1a'
import serial
import subprocess
from loguru import logger
import time
import codecs
from datetime import datetime, timedelta
from copy import copy

GSM7_BASIC = ('@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ\x1bÆæßÉ !\"#¤%&\'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ`¿abcdefghijklmnopqrstuvwxyzäöñüà')
GSM7_EXTENDED = {chr(0xFF): 0x0A,
                 # CR2: chr(0x0D),
                 '^': chr(0x14),
                 # SS2: chr(0x1B),
                 '{': chr(0x28),
                 '}': chr(0x29),
                 '\\': chr(0x2F),
                 '[': chr(0x3C),
                 '~': chr(0x3D),
                 ']': chr(0x3E),
                 '|': chr(0x40),
                 '€': chr(0x65)}
MAX_MESSAGE_LENGTH = {0x00: 160,  # GSM-7
                      0x04: 140,  # 8-bit
                      0x08: 70}  # UCS2
MAX_MULTIPART_MESSAGE_LENGTH = {0x00: 153,  # GSM-7
                                0x04: 133,  # 8-bit
                                0x08: 67}  # UCS2
rawStrToByteArray = lambda x: bytearray(bytes(x, 'latin-1'))



class InformationElement(object):
    # -*- encoding: utf-8 -*-
    def __new__(cls, *args, **kwargs): #iei, ieLen, ieData):
        """ Causes a new InformationElement class, or subclass
        thereof, to be created. If the IEI is recognized, a specific
        subclass of InformationElement is returned """
        if len(args) > 0:
            targetClass = IEI_CLASS_MAP.get(args[0], cls)
        elif 'iei' in kwargs:
            targetClass = IEI_CLASS_MAP.get(kwargs['iei'], cls)
        else:
            return super(InformationElement, cls).__new__(cls)
        return super(InformationElement, targetClass).__new__(targetClass)

    def __init__(self, iei, ieLen=0, ieData=None):
        self.id = iei # IEI
        self.dataLength = ieLen # IE Length
        self.data = ieData or [] # raw IE data

    @classmethod
    def decode(cls, byteIter):
        """ Decodes a single IE at the current position in the specified
        byte iterator

        :return: An InformationElement (or subclass) instance for the decoded IE
        :rtype: InformationElement, or subclass thereof
        """
        iei = next(byteIter)
        ieLen = next(byteIter)
        ieData = []
        for i in range(ieLen):
            ieData.append(next(byteIter))
        return InformationElement(iei, ieLen, ieData)

    def encode(self):
        """ Encodes this IE and returns the resulting bytes """
        result = bytearray()
        result.append(self.id)
        result.append(self.dataLength)
        result.extend(self.data)
        return result

    def __len__(self):
        """ Exposes the IE's total length (including the IEI and IE length octet) in octets """
        return self.dataLength + 2


class Concatenation(InformationElement):
    def __init__(self, iei=0x00, ieLen=0, ieData=None):
        super(Concatenation, self).__init__(iei, ieLen, ieData)
        if ieData != None:
            if iei == 0x00:  # 8-bit reference
                self.reference, self.parts, self.number = ieData
            else:  # 0x08: 16-bit reference
                self.reference = ieData[0] << 8 | ieData[1]
                self.parts = ieData[2]
                self.number = ieData[3]

    def encode(self):
        if self.reference > 0xFF:
            self.id = 0x08  # 16-bit reference
            self.data = [self.reference >> 8, self.reference & 0xFF, self.parts, self.number]
        else:
            self.id = 0x00  # 8-bit reference
            self.data = [self.reference, self.parts, self.number]
        self.dataLength = len(self.data)
        return super(Concatenation, self).encode()


class PortAddress(InformationElement):
    def __init__(self, iei=0x04, ieLen=0, ieData=None):
        super(PortAddress, self).__init__(iei, ieLen, ieData)
        if ieData != None:
            if iei == 0x04: # 8-bit port addressing scheme
                self.destination, self.source = ieData
            else: # 0x05: 16-bit port addressing scheme
                self.destination = ieData[0] << 8 | ieData[1]
                self.source = ieData[2] << 8 | ieData[3]

    def encode(self):
        if self.destination > 0xFF or self.source > 0xFF:
            self.id = 0x05 # 16-bit
            self.data = [self.destination >> 8, self.destination & 0xFF, self.source >> 8, self.source & 0xFF]
        else:
            self.id = 0x04 # 8-bit
            self.data = [self.destination, self.source]
        self.dataLength = len(self.data)
        return super(PortAddress, self).encode()


IEI_CLASS_MAP = {0x00: Concatenation, # Concatenated short messages, 8-bit reference number
                 0x08: Concatenation, # Concatenated short messages, 16-bit reference number
                 0x04: PortAddress, # Application port addressing scheme, 8 bit address
                 0x05: PortAddress # Application port addressing scheme, 16 bit address
                }


class Pdu(object):
    def __init__(self, data, tpduLength):
        """ Constructor
        :param data: the raw PDU data (as bytes)
        :type data: bytearray
        :param tpduLength: Length (in bytes) of the TPDU
        :type tpduLength: int
        """
        self.data = data
        self.tpduLength = tpduLength

    def __str__(self):
        return str(codecs.encode(self.data, 'hex_codec'), 'ascii').upper()


def encodeSmsSubmitPdu(number, text, reference=0, validity=None, smsc=None, requestStatusReport=True, rejectDuplicates=False, sendFlash=False):
    tpduFirstOctet = 0x01  # SMS-SUBMIT PDU
    if validity != None:
        # Validity period format (TP-VPF) is stored in bits 4,3 of the first TPDU octet
        if type(validity) == timedelta:
            # Relative (TP-VP is integer)
            tpduFirstOctet |= 0x10  # bit4 == 1, bit3 == 0
            validityPeriod = [_encodeRelativeValidityPeriod(validity)]
        elif type(validity) == datetime:
            # Absolute (TP-VP is semi-octet encoded date)
            tpduFirstOctet |= 0x18  # bit4 == 1, bit3 == 1
            validityPeriod = _encodeTimestamp(validity)
        else:
            raise TypeError('"validity" must be of type datetime.timedelta (for relative value) or datetime.datetime (for absolute value)')
    else:
        validityPeriod = None
    if rejectDuplicates:
        tpduFirstOctet |= 0x04  # bit2 == 1
    if requestStatusReport:
        tpduFirstOctet |= 0x20  # bit5 == 1

    # Encode message text and set data coding scheme based on text contents
    try:
        encodedTextLength = len(encodeGsm7(text))
    except ValueError:
        # Cannot encode text using GSM-7; use UCS2 instead
        encodedTextLength = len(text)
        alphabet = 0x08  # UCS2
    else:
        alphabet = 0x00  # GSM-7

    # Check if message should be concatenated
    if encodedTextLength > MAX_MESSAGE_LENGTH[alphabet]:
        # Text too long for single PDU - add "concatenation" User Data Header
        concatHeaderPrototype = Concatenation()
        concatHeaderPrototype.reference = reference

        # Devide whole text into parts
        if alphabet == 0x00:
            pduTextParts = divideTextGsm7(text)
        elif alphabet == 0x08:
            pduTextParts = divideTextUcs2(text)
        else:
            raise NotImplementedError

        pduCount = len(pduTextParts)
        concatHeaderPrototype.parts = pduCount
        tpduFirstOctet |= 0x40
    else:
        concatHeaderPrototype = None
        pduCount = 1

    # Construct required PDU(s)
    pdus = []
    for i in range(pduCount):
        pdu = bytearray()
        if smsc:
            pdu.extend(_encodeAddressField(smsc, smscField=True))
        else:
            pdu.append(0x00)  # Don't supply an SMSC number - use the one configured in the device

        udh = bytearray()
        if concatHeaderPrototype != None:
            concatHeader = copy(concatHeaderPrototype)
            concatHeader.number = i + 1
            pduText = pduTextParts[i]
            pduTextLength = len(pduText)
            udh.extend(concatHeader.encode())
        else:
            pduText = text

        udhLen = len(udh)

        pdu.append(tpduFirstOctet)
        pdu.append(reference)  # message reference
        # Add destination number
        pdu.extend(_encodeAddressField(number))
        pdu.append(0x00)  # Protocol identifier - no higher-level protocol

        pdu.append(alphabet if not sendFlash else (0x10 if alphabet == 0x00 else 0x18))
        if validityPeriod:
            pdu.extend(validityPeriod)

        if alphabet == 0x00:  # GSM-7
            encodedText = encodeGsm7(pduText)
            userDataLength = len(encodedText)  # Payload size in septets/characters
            if udhLen > 0:
                shift = ((udhLen + 1) * 8) % 7  # "fill bits" needed to make the UDH end on a septet boundary
                userData = packSeptets(encodedText, padBits=shift)
                if shift > 0:
                    userDataLength += 1  # take padding bits into account
            else:
                userData = packSeptets(encodedText)
        elif alphabet == 0x08:  # UCS2
            userData = encodeUcs2(pduText)
            userDataLength = len(userData)

        if udhLen > 0:
            userDataLength += udhLen + 1  # +1 for the UDH length indicator byte
            pdu.append(userDataLength)
            pdu.append(udhLen)
            pdu.extend(udh)  # UDH
        else:
            pdu.append(userDataLength)
        pdu.extend(userData)  # User Data (message payload)
        tpdu_length = len(pdu) - 1
        pdus.append(Pdu(pdu, tpdu_length))
    return pdus


def _encodeRelativeValidityPeriod(validityPeriod):
    seconds = validityPeriod.seconds + (validityPeriod.days * 24 * 3600)
    if seconds <= 43200:  # 12 hours
        tpVp = int(seconds / 300) - 1  # divide by 5 minutes, subtract 1
    elif seconds <= 86400:  # 24 hours
        tpVp = int((seconds - 43200) / 1800) + 143  # subtract 12 hours, divide by 30 minutes. add 143
    elif validityPeriod.days <= 30:  # 30 days
        tpVp = validityPeriod.days + 166  # amount of days + 166
    elif validityPeriod.days <= 441:  # max value of tpVp is 255
        tpVp = int(validityPeriod.days / 7) + 192  # amount of weeks + 192
    else:
        raise ValueError('Validity period too long; tpVp limited to 1 octet (max value: 255)')
    return tpVp


def _encodeTimestamp(timestamp):
    if timestamp.tzinfo == None:
        raise ValueError('Please specify time zone information for the timestamp (e.g. by using gsmmodem.util.SimpleOffsetTzInfo)')

    # See if the timezone difference is positive/negative
    tzDelta = timestamp.utcoffset()
    if tzDelta.days >= 0:
        tzValStr = '{0:0>2}'.format(int(tzDelta.seconds / 60 / 15))
    else:  # negative
        tzVal = int((tzDelta.days * -3600 * 24 - tzDelta.seconds) / 60 / 15)  # calculate offset in 0.25 hours
        # Cast as literal hex value and set MSB of first semi-octet of timezone to 1 to indicate negative value
        tzVal = int('{0:0>2}'.format(tzVal), 16) | 0x80
        tzValStr = '{0:0>2X}'.format(tzVal)

    dateStr = timestamp.strftime('%y%m%d%H%M%S') + tzValStr

    return encodeSemiOctets(dateStr)


def encodeSemiOctets(number):
    if len(number) % 2 == 1:
        number = number + 'F'  # append the "end" indicator
    octets = [int(number[i + 1] + number[i], 16) for i in range(0, len(number), 2)]
    return bytearray(octets)


def encodeGsm7(plaintext, discardInvalid=False):
    result = bytearray()
    plaintext = str(plaintext)

    for char in plaintext:
        idx = GSM7_BASIC.find(char)
        if idx != -1:
            result.append(idx)
        elif char in GSM7_EXTENDED:
            result.append(0x1B)  # ESC - switch to extended table
            result.append(ord(GSM7_EXTENDED[char]))
        elif not discardInvalid:
            raise ValueError('Cannot encode char "{0}" using GSM-7 encoding'.format(char))
    return result


def divideTextGsm7(plainText):
    result = []

    plainStartPtr = 0
    plainStopPtr = 0
    chunkByteSize = 0

    plainText = str(plainText)
    while plainStopPtr < len(plainText):
        char = plainText[plainStopPtr]
        idx = GSM7_BASIC.find(char)
        if idx != -1:
            chunkByteSize = chunkByteSize + 1;
        elif char in GSM7_EXTENDED:
            chunkByteSize = chunkByteSize + 2;
        else:
            raise ValueError('Cannot encode char "{0}" using GSM-7 encoding'.format(char))

        plainStopPtr = plainStopPtr + 1
        if chunkByteSize > MAX_MULTIPART_MESSAGE_LENGTH[0x00]:
            plainStopPtr = plainStopPtr - 1

        if chunkByteSize >= MAX_MULTIPART_MESSAGE_LENGTH[0x00]:
            result.append(plainText[plainStartPtr:plainStopPtr])
            plainStartPtr = plainStopPtr
            chunkByteSize = 0

    if chunkByteSize > 0:
        result.append(plainText[plainStartPtr:])

    return result


def divideTextUcs2(plainText):
    result = []
    resultLength = 0

    fullChunksCount = int(len(plainText) / MAX_MULTIPART_MESSAGE_LENGTH[0x08])
    for i in range(fullChunksCount):
        result.append(plainText[i * MAX_MULTIPART_MESSAGE_LENGTH[0x08]: (i + 1) * MAX_MULTIPART_MESSAGE_LENGTH[0x08]])
        resultLength = resultLength + MAX_MULTIPART_MESSAGE_LENGTH[0x08]

    # Add last, not fully filled chunk
    if resultLength < len(plainText):
        result.append(plainText[resultLength:])

    return result


def _encodeAddressField(address, smscField=False):
    toa = 0x80 | 0x00 | 0x01  # Type-of-address start | Unknown type-of-number | ISDN/tel numbering plan
    alphaNumeric = False
    if address.isalnum():
        # Might just be a local number
        if address.isdigit():
            # Local number
            toa |= 0x20
        else:
            # Alphanumeric address
            toa |= 0x50
            toa &= 0xFE  # switch to "unknown" numbering plan
            alphaNumeric = True
    else:
        if address[0] == '+' and address[1:].isdigit():
            # International number
            toa |= 0x10
            # Remove the '+' prefix
            address = address[1:]
        else:
            # Alphanumeric address
            toa |= 0x50
            toa &= 0xFE  # switch to "unknown" numbering plan
            alphaNumeric = True
    if alphaNumeric:
        addressValue = packSeptets(encodeGsm7(address, False))
        addressLen = len(addressValue) * 2
    else:
        addressValue = encodeSemiOctets(address)
        if smscField:
            addressLen = len(addressValue) + 1
        else:
            addressLen = len(address)
    result = bytearray()
    result.append(addressLen)
    result.append(toa)
    result.extend(addressValue)
    return result


def packSeptets(octets, padBits=0):
    result = bytearray()
    if type(octets) == str:
        octets = iter(rawStrToByteArray(octets))
    elif type(octets) == bytearray:
        octets = iter(octets)
    shift = padBits
    if padBits == 0:
        try:
            prevSeptet = next(octets)
        except StopIteration:
            return result
    else:
        prevSeptet = 0x00
    for octet in octets:
        septet = octet & 0x7f;
        if shift == 7:
            # prevSeptet has already been fully added to result
            shift = 0
            prevSeptet = septet
            continue
        b = ((septet << (7 - shift)) & 0xFF) | (prevSeptet >> shift)
        prevSeptet = septet
        shift += 1
        result.append(b)
    if shift != 7:
        # There is a bit "left over" from prevSeptet
        result.append(prevSeptet >> shift)
    return result


def encodeUcs2(text):
    result = bytearray()

    for b in map(ord, text):
        result.append(b >> 8)
        result.append(b & 0xFF)
    return result

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

def send_sms(phone, content, block=10):
    """
    phone: 发送的电话号码的数组，电话号码格式是字符串
    content： 发送的内容，也是字符串，可以中文和特殊字符
    block： 短信发送间隔，不要小于5s
    注意是事项：  1、serial调用不能太频繁，所以建议停30s
                2、长短信每段的发送也得停个10s
    """
    s = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ttyUSB0', baudrate=115200, timeout=100)
    s2 = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ttyUSB0', baudrate=115200, timeout=100)
    # s = SERIAL
    # s.write(('AT+CMGF=0' + TERMINATOR).encode())                                    # d18
    # logger.info(s.read(64))
    # AT+CMGF=0 告诉它你要发的是PDU类的短信）
    # 当需要连续调用+CMGS发送多条短信时，最好设置AT+CMMS=2
    for i in range(len(phone)):
        # 循环发给所提供的号码
        logger.info("begin phone " + str(i + 1) + " : " + phone[i] + " , content : " + content)
        pdus = encodeSmsSubmitPdu(phone[i], content)
        pdus_len = len(pdus)
        s.write(('ATZ' + TERMINATOR).encode())
        # s.write(('AT+CSCA="+8613800755500"' + TERMINATOR).encode())
        # logger.info(s.read(64))
        s.write(('AT+CMGF=0' + TERMINATOR).encode())
        logger.info(s.read(64))
        for j in range(pdus_len):
            s.write(('AT+CMGS={0}'.format(pdus[j].tpduLength) + TERMINATOR).encode())   # d30
            sleep(1)
            logger.info(s.read(64))
            s.write((str(pdus[j])).encode())  # d31
            s.write(b'\x1A\r\n')
            logger.info(s.read(64))
            logger.info("phone " + str(i + 1) + " : part " + str(j + 1) + " done , total : " + str(pdus_len))
            if j != pdus_len-1:
                sleep(block)
        # s.write(('ATZ' + TERMINATOR).encode())
    s.close()
    logger.info("serial close!")
    # sleep(30)


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

def sms_send(deliver_data):
    try:
        logger.info("receive job")
        # deliver_data = json.loads(str(deliver_data, encoding="utf-8"))
        cell_list = deliver_data['cells']
        content = deliver_data['content']
        send_sms_unclose(cell_list, content)
    except:
        logger.exception("sms_20201228 consume fail！")


def sms_send_old(ch, method, properties, body):
    try:
        body = json.loads(str(body, encoding="utf-8"))
        logger.info("receive job")
        cell_list = body['cells']
        content = body['content']
        send_sms(cell_list, content, 10)
    except:
        print("consume fail！")
        # logger.exception(f"{self.pre_msg}:发送短信失败")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def sms_send_test(ch, method, properties, body):
    try:
        body = json.loads(str(body, encoding="utf-8"))
        logger.info("receive job")
        cell_list = body['cells']
        content = body['content']
        send_sms_unclose(cell_list, content)
    except:
        print("consume fail！")
        # logger.exception(f"{self.pre_msg}:发送短信失败")
    ch.basic_ack(delivery_tag=method.delivery_tag)

class RabbitMQServer(object):
    def __init__(self, exchange=None, route_key=None, queue=None):
        self.connection = None
        self.channel = None
        self.exchange = exchange if exchange else "server_mq"
        self.route_key = route_key if route_key else "schedule"
        self.queue = queue if queue else "direct_scheduler"

    def init_connect(self):
        try:
            self.close_connect()
            credentials = pika.PlainCredentials("admin", "admin")
            parameters = pika.ConnectionParameters(
                host="192.168.1.99",
                port=5672,
                virtual_host="/",
                credentials=credentials,
                heartbeat=0
            )
            self.connection = pika.BlockingConnection(parameters)

            self.channel = self.connection.channel()

            if isinstance(self, RabbitConsumer):
                self.channel.basic_consume(
                    queue=self.queue,
                    on_message_callback=self.callback_func,
                    auto_ack=False
                )
        except Exception as e:
            logger.exception(e)

    def close_connect(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()


class RabbitConsumer(RabbitMQServer):
    def __init__(self, callback_func, queue=None):
        super(RabbitConsumer, self).__init__(queue=queue)
        self.callback_func = callback_func

    def start_consumer(self):
        while True:
            try:
                self.init_connect()
                self.channel.start_consuming()
            except (ConnectionClosed, ChannelClosed):
                time.sleep(1)
                self.init_connect()
            except Exception as e:
                time.sleep(1)
                logger.exception(f"RabbitConsumer发生错误->{e}")
                self.init_connect()


class RabbitPublisher(RabbitMQServer):
    def __init__(self, exchange=None, route_key=None):
        super(RabbitPublisher, self).__init__(exchange, route_key)
        self.init_connect()

    def publish(self, message):
        try:
            properties = pika.BasicProperties(
                delivery_mode=2,
                content_encoding="UTF-8",
                content_type="text/plain",
            )
            self.channel.basic_publish(exchange=self.exchange, routing_key=self.route_key, body=message,
                                       properties=properties)
        except (ConnectionClosed, ChannelClosed):
            time.sleep(1)
            self.init_connect()
        except Exception as e:
            logger.exception(f"RabbitPublisher发生错误->{e}")
            time.sleep(1)
            self.init_connect()


def consumer_test(ch, method, properties, body):
    try:
        # cell_list = body['cells']
        # context = body['context']
        print(body)
    except:
        print("consume fail！")
        # logger.exception(f"{self.pre_msg}:发送短信失败")
    time.sleep(10)
    ch.basic_ack(delivery_tag=method.delivery_tag)


publisher = RabbitPublisher(
    exchange="master_direct_exchange",
    route_key="master_schedule_router"
)
cnt1 = sys.argv[1]
cnt2 = sys.argv[2]
cnt1 = cnt1.split(',')

for i in range(int(cnt2)):
    # publisher.publish('{"cells": ["15801051609"], "content": "' + str(i+1) + ' -- 某某人与反馈疗法三级独立声卡的房间里看到到家乐福咖啡家里的事开发建设狄拉克附件弗兰克大家风范了巨大十分激烈某某人与反馈疗法三级独立声卡的房间里看到到家乐福咖啡家里的事开发建设狄拉克附件弗兰克大家风范了巨大十分激烈大噶快乐的感觉克拉的关键时刻劳动节快乐感觉劳动工具阿拉山口大概就是的卡拉格"}')  # 传字符串即可
    publisher.publish(json.dumps(eval('{"msg_list": ["externals.sms_send"],"deliver_data": {"cells": ' + str(cnt1) + ', "content": "【安乡大数据实战平台】某某人 mac(' + str(i+1) + '),于2020-xx-xx xx:xx:xx，出现在2组—潺陵路农商行"}}')))  # 传字符串即可
    # publisher.publish(json.dumps({"cells": ["' + phone + '"], "content": "【安乡大数据实战平台】某某人 mac(' + str(i+1) + '),于2020-xx-xx xx:xx:xx，出现在2组—潺陵路农商行"})  # 传字符串即可

# for i in range(int(cnt2)):
#     phone_tmp = (phone + ',') * int(cnt1)
#     # publisher.publish('{"cells": ["15801051609"], "content": "' + str(i+1) + ' -- 某某人与反馈疗法三级独立声卡的房间里看到到家乐福咖啡家里的事开发建设狄拉克附件弗兰克大家风范了巨大十分激烈某某人与反馈疗法三级独立声卡的房间里看到到家乐福咖啡家里的事开发建设狄拉克附件弗兰克大家风范了巨大十分激烈大噶快乐的感觉克拉的关键时刻劳动节快乐感觉劳动工具阿拉山口大概就是的卡拉格"}')  # 传字符串即可
#     publisher.publish(json.dumps({"msg_list": ["externals.sms_send",],"deliver_data": {"cells": [phone], "content": "【安乡大数据实战平台】某某人 mac(" + str(i+1) + "),于2020-xx-xx xx:xx:xx，出现在2组—潺陵路农商行"}}))  # 传字符串即可
#     # publisher.publish(json.dumps({"cells": ["' + phone + '"], "content": "【安乡大数据实战平台】某某人 mac(' + str(i+1) + '),于2020-xx-xx xx:xx:xx，出现在2组—潺陵路农商行"})  # 传字符串即可

# consumer = RabbitConsumer(sms_send_test, "cjh_test")
# consumer.start_consumer()
#
# s = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ttyUSB0', baudrate=115200, timeout=100)
