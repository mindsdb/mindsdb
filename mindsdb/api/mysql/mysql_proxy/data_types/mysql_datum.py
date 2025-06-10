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

import struct

from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    DEFAULT_CAPABILITIES,
    NULL_VALUE,
    ONE_BYTE_ENC,
    THREE_BYTE_ENC,
    TWO_BYTE_ENC,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)

NULL_VALUE_INT = ord(NULL_VALUE)


class Datum:
    __slots__ = ["value", "var_type", "var_len"]

    def __init__(self, var_type, value=None, var_len=None):
        # TODO other types: float, timestamp
        self.value = b""

        if var_len is None:
            idx = var_type.find("<")
            var_len = var_type[idx + 1 : -1]
            var_type = var_type[:idx]
        self.var_type = var_type
        self.var_len = var_len

        if value is not None:
            self.set(value)

    def set(self, value):
        self.value = value

    def setFromBuff(self, buff):
        if self.var_len == "lenenc":
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
                logger.error("Cant decode integer greater than 8 bytes")
                return buff[end - 1 :]  # noqa: E203

            for j in range(8 - (end - start)):
                num_str += b"\0"

            if self.var_type == "int":
                self.value = struct.unpack("i", num_str)
                return buff[end:]

            if self.var_type in ["byte", "string"]:
                length = struct.unpack("Q", num_str)[0]
                self.value = buff[end : (length + end)]  # noqa: E203
                return buff[(length + end) :]  # noqa: E203

        if self.var_len == "EOF":
            length = len(buff)
            self.var_len = str(length)
            self.value = buff
            return ""
        else:
            length = self.var_len

        if self.var_type == "string" and self.var_len == "NUL":
            for j, x in enumerate(buff):
                if int(x) == 0:
                    length = j + 1
                    break

        length = int(length)
        if self.var_type in ["byte", "string"]:
            end = length
            self.value = buff[:end]
        else:  # if its an integer
            end = length
            num_str = buff[:end]
            if end > 8:
                logger.error("cant decode integer greater than 8 bytes")
                return buff[end:]
            for j in range(8 - end):
                num_str += b"\0"
            self.value = struct.unpack("Q", num_str)[0]
        if str(self.var_len) == "NUL":
            self.value = self.value[:-1]
        return buff[end:]

    @classmethod
    def serialize_int(cls, value):
        if value is None:
            return NULL_VALUE

        byte_count = -(value.bit_length() // (-8))

        if byte_count == 0:
            return b"\0"
        if value < NULL_VALUE_INT:
            return struct.pack("B", value)
        if value >= NULL_VALUE_INT and byte_count <= 2:
            return TWO_BYTE_ENC + struct.pack("H", value)
        if byte_count <= 3:
            return THREE_BYTE_ENC + struct.pack("i", value)[:3]
        if byte_count <= 8:
            return THREE_BYTE_ENC + struct.pack("Q", value)

    def toStringPacket(self):
        return self.get_serializer()(self.value)

    def get_serializer(self):
        if self.var_type in ("string", "byte"):
            if self.var_len == "lenenc":
                if isinstance(self.value, bytes):
                    return self.serialize_bytes
                return self.serialize_str
            if self.var_len == "EOF":
                return self.serialize_str_eof
            if self.var_len == "NUL":
                return lambda v: bytes(v, "utf-8") + struct.pack("b", 0)
            if self.var_len == "packet":
                return lambda v: v.get_packet_string()
            else:
                return lambda v: struct.pack(self.var_len + "s", bytes(v, "utf-8"))[: int(self.var_len)]

        if self.var_type == "int":
            if self.var_len == "lenenc":
                return self.serialize_int
            else:
                return lambda v: struct.pack("Q", v)[: int(self.var_len)]

    @classmethod
    def serialize_str_eof(cls, value):
        length = len(value)
        var_len = length
        if length == 0:
            return b""
        else:
            return struct.pack("{len}s".format(len=var_len), bytes(value, "utf-8"))[:length]

    # def serialize_obj(self, value):
    #     return self.serialize_str(str(value))

    @classmethod
    def serialize_str(cls, value):
        return cls.serialize_bytes(value.encode("utf8"))

    @classmethod
    def serialize_bytes(cls, value):
        val_len = len(value)

        if val_len == 0:
            return b"\0"

        if val_len < NULL_VALUE_INT:
            return struct.pack("B", val_len) + value

        byte_count = -(val_len.bit_length() // (-8))
        if byte_count <= 2:
            return TWO_BYTE_ENC + struct.pack("H", val_len) + value
        if byte_count <= 3:
            return THREE_BYTE_ENC + struct.pack("i", val_len)[:3] + value
        if byte_count <= 8:
            return THREE_BYTE_ENC + struct.pack("Q", val_len) + value


def test():
    import pprint

    u = Datum("int<8>", DEFAULT_CAPABILITIES >> 16)
    pprint.pprint(u.toStringPacket())


# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()
