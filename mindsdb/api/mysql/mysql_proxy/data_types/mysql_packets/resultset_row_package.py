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
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import NULL_VALUE
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES


class ResultsetRowPacket(Packet):
    '''
    Implementation based on:
    https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
    https://mariadb.com/kb/en/resultset-row/
    '''

    @staticmethod
    def get_serializers(columns):
        serializers = []
        for column in columns:

            fnc = Datum('string', None, 'lenenc', is_empty=True).get_serializer()
            serializers.append(fnc)
        return serializers

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        serializers = self._kwargs['serializers']

        self._body = b''
        for i, item in enumerate(self._kwargs['data']):
            self._body += serializers[i](str(item))

        self._length = len(self._body)

    @property
    def body(self):
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(
            str(ResultsetRowPacket().get_packet_string())
        )


if __name__ == "__main__":
    ResultsetRowPacket.test()
