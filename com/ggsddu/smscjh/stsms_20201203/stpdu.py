# -*- encoding: utf-8 -*-
"""
@File       :   stpdu.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/11/24 10:36
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
# -*- encoding: utf-8 -*-
"""
@File       :   stpdu.py    
@Contact    :   ggsddu.com
@Modify Time:   2020/11/18 14:10
@Author     :   cjh
@Version    :   1.0
@Description :   None
"""
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