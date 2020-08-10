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

import logging
import struct

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (NULL_VALUE, TYPES)


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
            if data[i] is None:
                continue

            enc = ''
            col_type = col['type']
            if col_type == TYPES.MYSQL_TYPE_DOUBLE:
                enc = '<d'
            elif col_type == TYPES.MYSQL_TYPE_LONGLONG:
                enc = '<q'
            elif col_type == TYPES.MYSQL_TYPE_LONG:
                enc = '<l'
            elif col_type == TYPES.MYSQL_TYPE_FLOAT:
                enc = '<f'
            elif col_type == TYPES.MYSQL_TYPE_YEAR:
                enc = '<h'
            elif col_type == TYPES.MYSQL_TYPE_DATE:
                enc = ''
            elif col_type == TYPES.MYSQL_TYPE_TIMESTAMP:
                enc = ''
            elif col_type == TYPES.MYSQL_TYPE_DATETIME:
                enc = ''
            elif col_type == TYPES.MYSQL_TYPE_TIME:
                enc = ''
            elif col_type == TYPES.MYSQL_TYPE_NEWDECIMAL:
                enc = ''
            else:
                enc = 'string'

            if enc == '':
                raise Exception(f'Column with type {col_type} cant be encripted')

            if enc == 'string':
                self.value.append(Datum('string<lenenc>', str(data[i])).toStringPacket())
            else:
                self.value.append(struct.encode(enc, data[i]))[0]

    @property
    def body(self):
        string = b''.join(self.value)
        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        logging.basicConfig(level=10)
        pprint.pprint(
            str(BinaryResultsetRowPacket().getPacketString())
        )


if __name__ == "__main__":
    BinaryResultsetRowPacket.test()
