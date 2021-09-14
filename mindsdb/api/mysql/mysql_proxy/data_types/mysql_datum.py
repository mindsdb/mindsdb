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
import math
import struct

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import ONE_BYTE_ENC, TWO_BYTE_ENC, THREE_BYTE_ENC, NULL_VALUE, DEFAULT_CAPABILITIES
from mindsdb.api.mysql.mysql_proxy.utilities import log


class Datum():
    def __init__(self, type, value=None):
        self.type = type
        self.value = b''
        self.var_type = self.type.split('<')[0]
        self.var_len = self.type.split('<')[1].replace('>', '')

        if value is not None:
            self.set(value)

    def set(self, value):
        self.value = value

    def setFromBuff(self, buff):
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
                log.error('Cant decode integer greater than 8 bytes')
                return buff[end - 1:]

            for j in range(8 - (end - start)):
                num_str += b'\0'

            if self.var_type == 'int':
                self.value = struct.unpack('i', num_str)
                return buff[end:]

            if self.var_type in ['byte', 'string']:
                length = struct.unpack('Q', num_str)[0]
                self.value = buff[end:(length + end)]
                return buff[(length + end):]

        if self.var_len == 'EOF':
            length = len(buff)
            self.var_len = str(length)
            self.value = buff
            return ''
        else:
            length = self.var_len

        if self.type == 'string<NUL>':
            for j, x in enumerate(buff):
                if int(x) == 0:
                    length = j + 1
                    break

        length = int(length)
        if self.var_type in ['byte', 'string']:
            end = length
            self.value = buff[:end]
        else:  # if its an integer
            end = length
            num_str = buff[:end]
            if end > 8:
                log.error('cant decode integer greater than 8 bytes')
                return buff[end:]
            for j in range(8 - end):
                num_str += b'\0'
            self.value = struct.unpack('Q', num_str)[0]
        if str(self.var_len) == 'NUL':
            self.value = self.value[:-1]
        return buff[end:]

    def lenencInt(self, value):
        byte_count = int(math.ceil(math.log((value + 1), 2) / 8))
        if byte_count == 0:
            return b'\0'
        if value < NULL_VALUE[0]:
            return struct.pack('i', value)[:1]
        if value >= NULL_VALUE[0] and byte_count <= 2:
            return TWO_BYTE_ENC + struct.pack('i', value)[:2]
        if byte_count <= 3:
            return THREE_BYTE_ENC + struct.pack('i', value)[:3]
        if byte_count <= 8:
            return THREE_BYTE_ENC + struct.pack('Q', value)[:8]

    def toStringPacket(self):
        if self.type == 'string<packet>':
            return self.value.get_packet_string()

        if self.type in ['string<EOF>', 'byte<EOF>']:
            length = int(len(self.value))
            self.var_len = length
            if length == 0:
                return b''
            else:
                return struct.pack('{len}s'.format(len=self.var_len), bytes(self.value, 'utf-8'))[:length]

        if self.type == 'string<NUL>':
            return bytes(self.value, 'utf-8') + struct.pack('b', 0)

        if self.var_len.isdigit():
            length = int(self.var_len)

            if self.var_type == 'int':
                # little endian format
                return struct.pack('Q', self.value)[:length]
            if self.var_type == 'string':
                return struct.pack(self.var_len + 's', bytes(self.value, 'utf-8'))[:length]
            if self.var_type == 'byte':
                return struct.pack(self.var_len + 's', self.value)[:length]

        elif self.var_len == 'lenenc':
            if self.value is None:
                return NULL_VALUE
            if self.var_type == 'int':
                return self.lenencInt(self.value)

            if self.var_type in ['byte', 'string']:
                value = self.value.decode() if isinstance(self.value, bytes) else self.value
                if isinstance(value, str) is False and value is not None:
                    value = str(value)

                val_len = len(value.encode('utf8'))

                byte_count = int(math.ceil(math.log((val_len + 1), 2) / 8))
                if val_len < NULL_VALUE[0]:
                    return self.lenencInt(val_len) + bytes(value, 'utf-8')
                if val_len >= NULL_VALUE[0] and byte_count <= 2:
                    return TWO_BYTE_ENC + struct.pack('i', val_len)[:2] + bytes(value, 'utf-8')
                if byte_count <= 3:
                    return THREE_BYTE_ENC + struct.pack('i', val_len)[:3] + bytes(value, 'utf-8')
                if byte_count <= 8:
                    return THREE_BYTE_ENC + struct.pack('Q', val_len)[:8] + bytes(value, 'utf-8')


def test():
    import pprint

    u = Datum('int<8>', DEFAULT_CAPABILITIES >> 16)
    pprint.pprint(u.toStringPacket())


# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
