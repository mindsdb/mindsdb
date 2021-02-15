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

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import NULL_VALUE


class ResultsetRowPacket(Packet):
    '''
    Implementation based on:
    https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
    https://mariadb.com/kb/en/resultset-row/
    '''

    def setup(self):
        data = self._kwargs.get('data', {})
        self.value = []
        for val in data:
            if val is None:
                self.value.append(NULL_VALUE)
            else:
                self.value.append(Datum('string<lenenc>', str(val)))

    @property
    def body(self):
        string = b''
        for x in self.value:
            if x is NULL_VALUE:
                string += x
            else:
                string += x.toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(
            str(ResultsetRowPacket().getPacketString())
        )


if __name__ == "__main__":
    ResultsetRowPacket.test()
