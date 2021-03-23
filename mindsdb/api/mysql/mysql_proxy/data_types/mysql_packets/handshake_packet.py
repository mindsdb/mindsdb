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
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    DEFAULT_AUTH_METHOD,
    DEFAULT_COALLITION_ID,
    FILLER_FOR_WIRESHARK_DUMP,
    SERVER_STATUS_AUTOCOMMIT
)
from mindsdb.api.mysql.mysql_proxy.classes.server_capabilities import server_capabilities
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum


class HandshakePacket(Packet):
    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    def setup(self):
        capabilities = server_capabilities.value
        self.protocol_version = Datum('int<1>', 10)
        self.server_version = Datum('string<NUL>', '5.7.1-MindsDB-1.0')
        self.connection_id = Datum('int<4>', self.proxy.connection_id)
        self.scramble_1st_part = Datum('string<8>', self.proxy.salt[:8])
        self.reserved_byte = Datum('string<1>', '')
        self.server_capabilities_1st_part = Datum('int<2>', capabilities)
        self.server_default_collation = Datum('int<1>', DEFAULT_COALLITION_ID)
        self.status_flags = Datum('int<2>', SERVER_STATUS_AUTOCOMMIT)
        self.server_capabilities_2nd_part = Datum('int<2>', capabilities >> 16)
        self.wireshark_filler = Datum('int<1>', FILLER_FOR_WIRESHARK_DUMP)
        # self.wireshark_filler = Datum('int<1>', len(self.proxy.salt))
        self.reserved_filler1 = Datum('string<6>', '')
        self.reserved_filler2 = Datum('string<4>', '')
        self.scramble_2nd_part = Datum('string<NUL>', self.proxy.salt[8:])
        self.null_close = Datum('string<NUL>', DEFAULT_AUTH_METHOD)

    @property
    def body(self):
        order = [
            'protocol_version',
            'server_version',
            'connection_id',
            'scramble_1st_part',
            'reserved_byte',
            'server_capabilities_1st_part',
            'server_default_collation',
            'status_flags',
            'server_capabilities_2nd_part',
            'wireshark_filler',
            'reserved_filler1',
            'reserved_filler2',
            'scramble_2nd_part',
            'null_close'
        ]
        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(str(HandshakePacket().getPacketString()))


# only run the test if this file is called from debugger
if __name__ == "__main__":
    HandshakePacket.test()
