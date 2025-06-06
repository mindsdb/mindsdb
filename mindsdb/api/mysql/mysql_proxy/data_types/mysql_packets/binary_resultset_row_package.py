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

import datetime as dt
import struct

import pandas as pd

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES


class BinaryResultsetRowPacket(Packet):
    """
    Implementation based on:
    https://mariadb.com/kb/en/resultset-row/#binary-resultset-row
    https://dev.mysql.com/doc/internals/en/null-bitmap.html
    """

    def setup(self):
        data = self._kwargs.get("data", {})
        columns = self._kwargs.get("columns", {})

        self.value = [b"\x00"]

        # NOTE: according to mysql's doc offset=0 only for COM_STMT_EXECUTE, mariadb's doc does't mention that
        # but in fact it looks like offset=2 everywhere
        offset = 2
        nulls_bitmap = bytearray((len(columns) + offset + 7) // 8)
        for i, el in enumerate(data):
            if el is not None:
                continue
            byte_index = (i + offset) // 8
            bit_index = (i + offset) % 8
            nulls_bitmap[byte_index] |= 1 << bit_index
        self.value.append(bytes(nulls_bitmap))

        for i, col in enumerate(columns):
            # NOTE at this moment all types sends as strings, and it works
            val = data[i]
            if val is None:
                continue

            enc = None
            env_val = None
            col_type = col["type"]
            if col_type == TYPES.MYSQL_TYPE_DOUBLE:
                enc = "<d"
                val = float(val)
            elif col_type == TYPES.MYSQL_TYPE_LONGLONG:
                enc = "<q"
                val = int(val)
            elif col_type == TYPES.MYSQL_TYPE_LONG:
                enc = "<l"
                val = int(val)
            elif col_type == TYPES.MYSQL_TYPE_FLOAT:
                enc = "<f"
                val = float(val)
            elif col_type == TYPES.MYSQL_TYPE_YEAR:
                enc = "<h"
                val = int(float(val))
            elif col_type == TYPES.MYSQL_TYPE_SHORT:
                enc = "<h"
                val = int(val)
            elif col_type == TYPES.MYSQL_TYPE_TINY:
                enc = "<B"
                val = int(val)
            elif col_type == TYPES.MYSQL_TYPE_DATE:
                env_val = self.encode_date(val)
            elif col_type == TYPES.MYSQL_TYPE_TIMESTAMP:
                env_val = self.encode_date(val)
            elif col_type == TYPES.MYSQL_TYPE_DATETIME:
                env_val = self.encode_date(val)
            elif col_type == TYPES.MYSQL_TYPE_TIME:
                env_val = self.encode_time(val)
            elif col_type == TYPES.MYSQL_TYPE_NEWDECIMAL:
                enc = "string"
            elif col_type == TYPES.MYSQL_TYPE_VECTOR:
                enc = "byte"
            elif col_type == TYPES.MYSQL_TYPE_JSON:
                # json have to be encoded as byte<lenenc>, but actually for json there is no differ with string<>
                enc = "string"
            else:
                enc = "string"

            if enc == "":
                raise Exception(f"Column with type {col_type} cant be encripted")

            if enc == "byte":
                self.value.append(Datum("string", val, "lenenc").toStringPacket())
            elif enc == "string":
                if not isinstance(val, str):
                    val = str(val)
                self.value.append(Datum("string", val, "lenenc").toStringPacket())
            else:
                if env_val is None:
                    env_val = struct.pack(enc, val)
                self.value.append(env_val)

    def encode_time(self, val: dt.time | str) -> bytes:
        """https://mariadb.com/kb/en/resultset-row/#time-binary-encoding"""
        if isinstance(val, str):
            try:
                val = dt.datetime.strptime(val, "%H:%M:%S").time()
            except ValueError:
                val = dt.datetime.strptime(val, "%H:%M:%S.%f").time()
        if val == dt.time(0, 0, 0):
            return struct.pack("<B", 0)  # special case for 0 time
        out = struct.pack("<B", 0)  # positive time
        out += struct.pack("<L", 0)  # days
        out += struct.pack("<B", val.hour)
        out += struct.pack("<B", val.minute)
        out += struct.pack("<B", val.second)
        if val.microsecond > 0:
            out += struct.pack("<L", val.microsecond)
            len_bit = struct.pack("<B", 12)
        else:
            len_bit = struct.pack("<B", 8)
        return len_bit + out

    def encode_date(self, val):
        # date_type = None
        # date_value = None

        if isinstance(val, str):
            forms = [
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
            ]
            for f in forms:
                try:
                    date_value = dt.datetime.strptime(val, f)
                    break
                except ValueError:
                    date_value = None
            if date_value is None:
                raise ValueError(f"Invalid date format: {val}")
            date_type = "datetime"
        elif isinstance(val, pd.Timestamp):
            date_value = val
            date_type = "datetime"

        out = struct.pack("<H", date_value.year)
        out += struct.pack("<B", date_value.month)
        out += struct.pack("<B", date_value.day)

        if date_type == "datetime":
            out += struct.pack("<B", date_value.hour)
            out += struct.pack("<B", date_value.minute)
            out += struct.pack("<B", date_value.second)
            out += struct.pack("<L", date_value.microsecond)

        len_bit = struct.pack("<B", len(out))
        return len_bit + out

    @property
    def body(self):
        string = b"".join(self.value)
        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint

        pprint.pprint(str(BinaryResultsetRowPacket().get_packet_string()))


if __name__ == "__main__":
    BinaryResultsetRowPacket.test()
