"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

# import logging
from libs.helpers.logging import logging

import math
import struct

from libs.constants.mysql import *



class Datum():

    def __init__(self, type, value = None):
        self.type = type
        self.value = b''
        self.var_type = self.type.split('<')[0]
        self.var_len = self.type.split('<')[1].replace('>', '')

        if value is not None:
            self.set(value)

    def set(self, value):
        self.value = value

    def setFromBuff(self, buff):
        val = ''
        start = 0

        if self.var_len == 'lenenc':
            start = 1
            ln_enc = buff[0]
            if int(ln_enc) <= ONE_BYTE_ENC[0]:
                start = 0
                end = 1
            elif int(ln_enc) == TWO_BYTE_ENC[0]:
                end = 3
            elif int(ln_enc) == THREE_BYTE_ENC[0]:
                end = 4
            elif ln_enc:
                end = 9

            num_str = buff[start:end]
            if end > 9:
                logging.error('Cant decode integer greater than 8 bytes')
                return buff[end-1:]

            for j in range(8 - (start- end)):
                num_str += b'\0'

            if self.var_type == 'int':
                self.value = struct.unpack('i', num_str)
                return buff[end:]

            if self.var_type in ['byte', 'string']:
                length = struct.unpack('i', num_str)
                self.value = buff[start:(length+end)]
                return buff[(length+end):]

        if self.var_len == 'EOF':
            length = len(buff)
            self.var_len = str(length)
            self.value = buff
            return ''
        else:
            length = self.var_len

        if self.type == 'string<NUL>':
            for j,x in enumerate(buff):

                if int(x) == 0:
                    length = j+1
                    break

        length = int(length)
        if self.var_type in ['byte', 'string']:
            end = length
            self.value = buff[:end]
        else: # if its an integer
            end = length
            num_str = buff[:end]
            if end > 8:
                logging.error('cant decode integer greater than 8 bytes')
                return buff[end:]
            for j in range(8-end):
                num_str += b'\0'
            self.value = struct.unpack('Q', num_str)[0]
        if str(self.var_len) == 'NUL':
            self.value = self.value[:-1]
        return buff[end:]






    def toStringPacket(self):
        if self.type == 'string<packet>':
            return self.value.getPacketString()

        if self.type in ['string<EOF>', 'byte<EOF>']:
            length = int(len(self.value))
            self.var_len = length
            if length == 0:
                return b''
            else:
                return struct.pack('{len}s'.format(len=self.var_len), bytes(self.value, 'utf-8'))[:length]

        if self.type == 'string<NUL>':
            return  bytes(self.value, 'utf-8')+struct.pack('b',0)

        if self.var_len.isdigit():
            length = int(self.var_len)

            if self.var_type == 'int':
                #little endian format
                return struct.pack('Q', self.value)[:length]
            if self.var_type == 'string':
                return struct.pack(self.var_len+'s',bytes(self.value, 'utf-8'))[:length]
            if self.var_type == 'byte':
                return struct.pack(self.var_len+'s',self.value)[:length]

        elif self.var_len == 'lenenc':

            if self.value == None:
                return NULL_VALUE
            if self.var_type == 'int':
                byte_count = int(math.ceil(math.log((self.value+1),2)/8))
                if byte_count == 0:
                    return b'\0'
                if self.value < NULL_VALUE[0]:
                    return struct.pack('i', self.value)[:1]
                if self.value >= NULL_VALUE[0] and byte_count <= 2:
                    return TWO_BYTE_ENC + struct.pack('i', self.value)[:2]
                if byte_count <= 3:
                    return THREE_BYTE_ENC + struct.pack('i', self.value)[:3]
                if byte_count <= 8:
                    return THREE_BYTE_ENC + struct.pack('Q', self.value)[:8]

            if self.var_type in ['byte', 'string']:
                val_len = len(self.value)
                byte_count = int(math.ceil(math.log((val_len + 1), 2) / 8))
                if val_len < NULL_VALUE[0]:
                    return  struct.pack('b', val_len)[:1] + bytes(self.value, 'utf-8')
                if val_len >= NULL_VALUE[0] and byte_count <= 2:
                    return TWO_BYTE_ENC + struct.pack('i', val_len)[:2] +  bytes(self.value, 'utf-8')
                if byte_count <= 3:
                    return THREE_BYTE_ENC + struct.pack('i', val_len)[:3] +  bytes(self.value, 'utf-8')
                if byte_count <= 8:
                    return THREE_BYTE_ENC + struct.pack('Q', val_len)[:8] +  bytes(self.value, 'utf-8')



def test():


    import pprint

    logging.basicConfig(level=10)
    u = Datum('int<8>',DEFAULT_CAPABILITIES >> 16)
    pprint.pprint(u.toStringPacket())

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()