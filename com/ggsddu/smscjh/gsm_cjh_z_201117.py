# -*- coding: utf-8 -*-
from time import sleep
import serial
import codecs
from datetime import datetime, timedelta
from copy import copy

TERMINATOR = '\r'
CTRLZ = '\x1a'
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
                                0x04: 133,  # 8-bit TODO: Check this value!
                                0x08: 67}  # UCS2
rawStrToByteArray = lambda x: bytearray(bytes(x, 'latin-1'))


# d1 = "ATZ"              # 01 210 reset configuration
# d2 = 'ATE0'             # 01 221 echo off
# d3 = 'AT+CFUN?'         # 01 223 example response: +CFUN: 1, 判断是否为飞行模式
# d4 = 'AT+CMEE=1'        # 01 229 enable detailed error messages (even if it has already been set - ATZ may reset this)
# d5 = 'AT+CPIN?'         # 01 415 Unlock the SIM card if needed, 不行的话write('AT+CPIN="{0}"'.format(pin))
# d6 = 'AT+CLAC'          # 01 551 AT+CLAC responses differ between modems. Most respond with +CLAC: and then a comma-separated list of commands，返回所有at指令
#                         # while others simply return each command on a new line, with no +CLAC: prefix
# d7 = 'AT'               # 01 569 Check if modem is still alive
# # 248  cvoice - cnum : Try interactive command recognition
# d8 = 'AT^CVOICE=?'      # 01 Enable voice calls or Compose AT command that will read values under specified function, If there are values inside response - add command to the list
# d9 = 'AT+VTS=?'         # 01
# d10= 'AT^DTMF=?'        # 01
# d11= 'AT^USSDMODE=?'    # 01 Enable Huawei text-mode USSD
# d12= 'AT+WIND=?'        # 01 Check current WIND value; example response: +WIND: 63
# d13= 'AT+ZPAS=?'        # 01 See if this is a ZTE modem that has not yet been identified based on supported commands
# d14= 'AT+CSCS=?'        # 01 Get available encoding names
# d15= 'AT+CNUM=?'        # 01
#                         # Query subscriber phone number.
#                         # It must be stored on SIM by operator.
#                         # If is it not stored already, it usually is possible to store the number by user.
#                         # :raise TimeoutException: if a timeout was specified and reached
#                         # :return: Subscriber SIM phone number. Returns None if not known
#                         # :rtype: int
#
# d16 = 'AT+WIND?'                # 01 Check current WIND value; example response: +WIND: 63
# d17 = 'AT+COPS=3,0'             # 01 329 Use long alphanumeric name format
# d18 = 'AT+CMGF=0'               # 11111 332 SMS setup or Set to True for the modem to use text mode for SMS, or False for it to use PDU mode or Switch to text or PDU mode for SMS messages
# d19 = 'AT+CSCA?'                # 01 335 return: The default SMSC number stored on the SIM card or Set default SMSC number
# d20 = 'AT+CSMP=49,167,0,0'      # 01 343 Enable delivery reports
# d21 = 'AT+CSCA?'                # 01 return: The default SMSC number stored on the SIM card
# d22 = 'AT+CPMS=?'               # 01 352 Set message storage, but first check what the modem supports - example response: +CPMS: (("SM","BM","SR"),("SM"))
# d23 = 'AT+CPMS="SM","SM"'       # 01 377 Set message storage
# d24 = 'AT+CNMI=2,1,0,2'         # 01 383 Set message notifications, using TE for delivery reports <ds>
# d25 = 'AT+CLIP=1'               # 01 394 Enable calling line identification presentation
# d26 = 'AT+CRC=1'                # 01 401 Enable extended format of incoming indication (optional)
# d27 = 'AT+CVHU=0'               # 01 409 Enable call hang-up with ATH command (ignore if command not supported)
# # 以上是connect，以下是sendsms
# d28 = 'AT+CSCS=?'               # 01 Get available encoding names
# d29 = 'AT+CSCS="GSM"'           # 01 选择编码方式
# d30 = 'AT+CMGS=21'              # 11111 925 response: ['> ']
# d31 = '0021000BA15108011506F90008087FA453D16D88606F'      # 11111
# s = serial.Serial(dsrdtr=True, rtscts=True, port='/dev/ptmx', baudrate=115200, timeout=100)
# a = s.write((d1 + TERMINATOR).encode())
# a = s.write((d2 + TERMINATOR).encode())
# a = s.write((d3 + TERMINATOR).encode())
# a = s.write((d4 + TERMINATOR).encode())
# a = s.write((d5 + TERMINATOR).encode())
# a = s.write((d6 + TERMINATOR).encode())
# a = s.write((d7 + TERMINATOR).encode())
# a = s.write((d8 + TERMINATOR).encode())
# a = s.write((d9 + TERMINATOR).encode())
# a = s.write((d10 + TERMINATOR).encode())
# a = s.write((d11 + TERMINATOR).encode())
# a = s.write((d12 + TERMINATOR).encode())
# a = s.write((d13 + TERMINATOR).encode())
# a = s.write((d14 + TERMINATOR).encode())
# a = s.write((d15 + TERMINATOR).encode())
# a = s.write((d16 + TERMINATOR).encode())
# a = s.write((d17 + TERMINATOR).encode())
# a = s.write((d18 + TERMINATOR).encode())
# a = s.write((d19 + TERMINATOR).encode())
# a = s.write((d20 + TERMINATOR).encode())
# a = s.write((d21 + TERMINATOR).encode())
# a = s.write((d22 + TERMINATOR).encode())
# a = s.write((d23 + TERMINATOR).encode())
# a = s.write((d24 + TERMINATOR).encode())
# a = s.write((d25 + TERMINATOR).encode())
# a = s.write((d26 + TERMINATOR).encode())
# a = s.write((d27 + TERMINATOR).encode())
# a = s.write((d28 + TERMINATOR).encode())
# a = s.write((d29 + TERMINATOR).encode())
# a = s.write((d30 + TERMINATOR).encode())
# a = s.write((d31 + CTRLZ).encode())


def encodeSmsSubmitPdu(number, text, reference=0, validity=None, smsc=None, requestStatusReport=True, rejectDuplicates=False, sendFlash=False):
    """ Creates an SMS-SUBMIT PDU for sending a message with the specified text to the specified number

    :param number: the destination mobile number
    :type number: str
    :param text: the message text
    :type text: str
    :param reference: message reference number (see also: rejectDuplicates parameter)
    :type reference: int
    :param validity: message validity period (absolute or relative)
    :type validity: datetime.timedelta (relative) or datetime.datetime (absolute)
    :param smsc: SMSC number to use (leave None to use default)
    :type smsc: str
    :param rejectDuplicates: Flag that controls the TP-RD parameter (messages with same destination and reference may be rejected if True)
    :type rejectDuplicates: bool

    :return: A list of one or more tuples containing the SMS PDU (as a bytearray, and the length of the TPDU part
    :rtype: list of tuples
    """
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
    """ Encodes the specified relative validity period timedelta into an integer for use in an SMS PDU
    (based on the table in section 9.2.3.12 of GSM 03.40)

    :param validityPeriod: The validity period to encode
    :type validityPeriod: datetime.timedelta
    :rtype: int
    """
    # Python 2.6 does not have timedelta.total_seconds(), so compute it manually
    # seconds = validityPeriod.total_seconds()
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
    """ Encodes a 7-octet timestamp from the specified date

    Note: the specified timestamp must have a UTC offset set; you can use gsmmodem.util.SimpleOffsetTzInfo for simple cases

    :param timestamp: The timestamp to encode
    :type timestamp: datetime.datetime

    :return: The encoded timestamp
    :rtype: bytearray
    """
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
    """ Semi-octet encoding algorithm (e.g. for phone numbers)

    :return: bytearray containing the encoded octets
    :rtype: bytearray
    """
    if len(number) % 2 == 1:
        number = number + 'F'  # append the "end" indicator
    octets = [int(number[i + 1] + number[i], 16) for i in range(0, len(number), 2)]
    return bytearray(octets)


def encodeGsm7(plaintext, discardInvalid=False):
    """ GSM-7 text encoding algorithm

    Encodes the specified text string into GSM-7 octets (characters). This method does not pack
    the characters into septets.

    :param text: the text string to encode
    :param discardInvalid: if True, characters that cannot be encoded will be silently discarded

    :raise ValueError: if the text string cannot be encoded using GSM-7 encoding (unless discardInvalid == True)

    :return: A bytearray containing the string encoded in GSM-7 encoding
    :rtype: bytearray
    """
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


class InformationElement(object):
    """ User Data Header (UDH) Information Element (IE) implementation

    This represents a single field ("information element") in the PDU's
    User Data Header. The UDH itself contains one or more of these
    information elements.

    If the IEI (IE identifier) is recognized, the class will automatically
    specialize into one of the subclasses of InformationElement,
    e.g. Concatenation or PortAddress, allowing the user to easily
    access the specific (and useful) attributes of these special cases.
    """


class Concatenation(InformationElement):
    """ IE that indicates SMS concatenation.

    This implementation handles both 8-bit and 16-bit concatenation
    indication, and exposes the specific useful details of this
    IE as instance variables.

    Exposes:

    reference
        CSMS reference number, must be same for all the SMS parts in the CSMS
    parts
        total number of parts. The value shall remain constant for every short
        message which makes up the concatenated short message. If the value is zero then
        the receiving entity shall ignore the whole information element
    number
        this part's number in the sequence. The value shall start at 1 and
        increment for every short message which makes up the concatenated short message
    """

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


def divideTextGsm7(plainText):
    """ GSM7 message dividing algorithm

    Divides text into list of chunks that could be stored in a single, GSM7-encoded SMS message.

    :param plainText: the text string to divide
    :type plainText: str

    :return: A list of strings
    :rtype: list of str
    """
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
    """ UCS-2 message dividing algorithm

    Divides text into list of chunks that could be stored in a single, UCS-2 -encoded SMS message.

    :param plainText: the text string to divide
    :type plainText: str

    :return: A list of strings
    :rtype: list of str
    """
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
    """ Encodes the address into an address field

    :param address: The address to encode (phone number or alphanumeric)
    :type byteIter: str

    :return: Encoded SMS PDU address field
    :rtype: bytearray
    """
    # First, see if this is a number or an alphanumeric string
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
    """ Packs the specified octets into septets

    Typically the output of encodeGsm7 would be used as input to this function. The resulting
    bytearray contains the original GSM-7 characters packed into septets ready for transmission.

    :rtype: bytearray
    """
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
    """ UCS2 text encoding algorithm

    Encodes the specified text string into UCS2-encoded bytes.

    :param text: the text string to encode

    :return: A bytearray containing the string encoded in UCS2 encoding
    :rtype: bytearray
    """
    result = bytearray()

    for b in map(ord, text):
        result.append(b >> 8)
        result.append(b & 0xFF)
    return result


class Pdu(object):
    """ Encoded SMS PDU. Contains raw PDU data and related meta-information """

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
        for i in range(len(phone)):
            # 循环发给所提供的号码
            pdus = encodeSmsSubmitPdu(phone[i], content)
            for pdu in pdus:
                s.write(('AT+CMGS={0}'.format(pdu.tpduLength) + TERMINATOR).encode())  # d30
                s.write((str(pdu) + CTRLZ).encode())  # d31
                sleep(block)
            sleep(block)

# phone_num = ['15801051609', '15201259733']
phone_num = ['15801051609']
content = '【安乡大数据实战平台】某某人 mac(C8-EE-A6-3F-02-45),于2020-09-20 09:01:00，出现在阳光华庭南门车辆识别进出口gdagdasgsdagsda'
send_sms(phone_num, content, 10)
