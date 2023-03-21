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
    '''
    Implementation based on:
    https://mariadb.com/kb/en/resultset-row/#binary-resultset-row
    https://dev.mysql.com/doc/internals/en/null-bitmap.html
    '''

    def setup(self):
        data = self._kwargs.get('data', {})
        columns = self._kwargs.get('columns', {})

        self.value = [b'\x00']
        nulls = [0]
        for i, el in enumerate(data):
            if i > 0 and (i + 2) % 8 == 0:
                nulls.append(0)
            if el is None:
                if i < 6:
                    nulls[-1] = nulls[-1] + (1 << ((i + 2) % 8))
                else:
                    nulls[-1] = nulls[-1] + (1 << ((i - 6) % 8))
        self.value.append(bytes(nulls))

        for i, col in enumerate(columns):
            # NOTE at this moment all types sends as strings, and it works
            val = data[i]
            if val is None:
                continue

            enc = None
            env_val = None
            col_type = col['type']
            if col_type == TYPES.MYSQL_TYPE_DOUBLE:
                enc = '<d'
                val = float(val)
            elif col_type == TYPES.MYSQL_TYPE_LONGLONG:
                enc = '<q'
                val = int(float(val))
            elif col_type == TYPES.MYSQL_TYPE_LONG:
                enc = '<l'
                val = int(float(val))
            elif col_type == TYPES.MYSQL_TYPE_FLOAT:
                enc = '<f'
                val = float(val)
            elif col_type == TYPES.MYSQL_TYPE_YEAR:
                enc = '<h'
                val = int(float(val))
            elif col_type == TYPES.MYSQL_TYPE_DATE:
                env_val = self.encode_date(val)
            elif col_type == TYPES.MYSQL_TYPE_TIMESTAMP:
                env_val = self.encode_date(val)
            elif col_type == TYPES.MYSQL_TYPE_DATETIME:
                env_val = self.encode_date(val)
            elif col_type == TYPES.MYSQL_TYPE_TIME:
                enc = ''
            elif col_type == TYPES.MYSQL_TYPE_NEWDECIMAL:
                enc = ''
            else:
                enc = 'string'

            if enc == '':
                raise Exception(f'Column with type {col_type} cant be encripted')

            if enc == 'string':
                self.value.append(Datum('string<lenenc>', str(val)).toStringPacket())
            else:
                if env_val is None:
                    env_val = struct.pack(enc, val)
                self.value.append(env_val)

    def encode_date(self, val):
        # date_type = None
        # date_value = None

        if isinstance(val, str):
            try:
                date_value = dt.datetime.strptime(val, '%Y-%m-%d')
                date_type = 'date'
            except ValueError:
                try:
                    date_value = dt.datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')
                    date_type = 'datetime'
                except ValueError:
                    date_value = dt.datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%f')
                    date_type = 'datetime'
        elif isinstance(val, pd.Timestamp):
            date_value = val
            date_type = 'datetime'

        out = struct.pack('<H', date_value.year)
        out += struct.pack('<B', date_value.month)
        out += struct.pack('<B', date_value.day)

        if date_type == 'datetime':
            out += struct.pack('<B', date_value.hour)
            out += struct.pack('<B', date_value.minute)
            out += struct.pack('<B', date_value.second)
            out += struct.pack('<L', date_value.microsecond)

        len_bit = struct.pack('<B', len(out))
        return len_bit + out

    @property
    def body(self):
        string = b''.join(self.value)
        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(
            str(BinaryResultsetRowPacket().get_packet_string())
        )


if __name__ == "__main__":
    BinaryResultsetRowPacket.test()
