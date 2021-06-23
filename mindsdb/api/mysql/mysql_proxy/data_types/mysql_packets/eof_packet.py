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

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum


class EofPacket(Packet):
    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    def setup(self):
        status = 0 if 'status' not in self._kwargs else self._kwargs['status']
        self.eof_header = Datum('int<1>', int('0xfe', 0))
        self.warning_count = Datum('int<2>', 0)
        self.server_status = Datum('int<2>', status)

    @property
    def body(self):

        order = [
            'eof_header',
            'warning_count',
            'server_status'
        ]

        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(str(EofPacket().get_packet_string()))


# only run the test if this file is called from debugger
if __name__ == "__main__":
    EofPacket.test()
