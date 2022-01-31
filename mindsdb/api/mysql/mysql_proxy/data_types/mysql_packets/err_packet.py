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


class ErrPacket(Packet):
    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    # def setup(self):
    def setup(self, length=0, count_header=1, body=''):
        if length == 0:
            return

        self._length = length
        self._seq = count_header
        self._body = body

        err_code = None
        if 'err_code' in self._kwargs:
            err_code = self._kwargs['err_code']
        msg = None
        if 'msg' in self._kwargs:
            msg = self._kwargs['msg']

        self.err_header = Datum('int<1>', 255)

        if err_code is not None:
            self.err_code = Datum('int<2>', err_code)
        else:
            self.err_code = Datum('int<2>')

        if msg is not None:
            self.msg = Datum('string<EOF>', msg)
        else:
            self.msg = Datum('string<EOF>')

        for attr in (self.err_header, self.err_code, self.msg):
            self._body = attr.setFromBuf(self._body)

    @property
    def body(self):

        order = [
            'err_header',
            'err_code',
            'msg'
        ]
        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(str(ErrPacket().get_packet_string()))


# only run the test if this file is called from debugger
if __name__ == "__main__":
    ErrPacket.test()
