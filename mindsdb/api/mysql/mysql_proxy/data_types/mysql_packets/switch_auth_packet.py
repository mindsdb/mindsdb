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

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum


class SwitchOutPacket(Packet):
    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    def setup(self):
        status = 0 if 'status' not in self._kwargs else self._kwargs['status']
        seed = self._kwargs['seed']
        method = self._kwargs['method']
        self.eof_header = Datum('int<1>', int('0xfe',0))
        self.authentication_plugin_name = Datum('string<NUL>', method)
        self.seed = Datum('string<NUL>', seed)

    @property
    def body(self):

        order = [
            'eof_header',
            'authentication_plugin_name',
            'seed'
        ]

        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        logging.basicConfig(level=10)
        pprint.pprint(str(SwitchOutPacket().getPacketString()))


if __name__ == "__main__":
    SwitchOutPacket.test()
