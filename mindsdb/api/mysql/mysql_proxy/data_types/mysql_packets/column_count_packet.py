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


class ColumnCountPacket(Packet):
    def setup(self):
        count = self._kwargs.get('count', 0)
        self.column_count = Datum('int<lenenc>', count)

    @property
    def body(self):

        order = [
            'column_count'
        ]

        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(
            str(ColumnCountPacket(count=1).getPacketString())
        )


if __name__ == "__main__":
    ColumnCountPacket.test()
