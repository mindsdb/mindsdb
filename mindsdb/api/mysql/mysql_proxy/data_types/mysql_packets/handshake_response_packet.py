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

# https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
from mindsdb.api.mysql.mysql_proxy.external_libs.mysql_scramble import scramble
from mindsdb.api.mysql.mysql_proxy.classes.client_capabilities import ClentCapabilities
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CAPABILITIES
from mindsdb.api.mysql.mysql_proxy.classes.server_capabilities import server_capabilities


class HandshakeResponsePacket(Packet):
    '''
    Implementation based on description:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    def setup(self, length=0, count_header=1, body=''):
        length = len(body)

        if length == 0:
            return

        self.salt = self.proxy.salt

        self._length = length
        self._seq = count_header
        self._body = body

        self.scramble_func = scramble

        self.capabilities = Datum('int<4>')
        self.max_packet_size = Datum('int<4>')
        self.reserved = Datum('string<23>')
        self.username = Datum('string<NUL>')

        self.enc_password = Datum('string<NUL>')
        self.database = Datum('string<NUL>')

        self.charset = Datum('int<1>')

        self.client_auth_plugin = Datum('string<NUL>')

        buffer = body

        if len(body) == 32 and body[9:] == (b'\x00' * 23):
            self.type = 'SSLRequest'
            buffer = self.capabilities.setFromBuff(buffer)
            buffer = self.max_packet_size.setFromBuff(buffer)
            buffer = self.charset.setFromBuff(buffer)
        else:
            self.type = 'HandshakeResponse'
            buffer = self.capabilities.setFromBuff(buffer)
            capabilities = ClentCapabilities(self.capabilities.value)
            buffer = self.max_packet_size.setFromBuff(buffer)
            buffer = self.charset.setFromBuff(buffer)
            buffer = self.reserved.setFromBuff(buffer)
            buffer = self.username.setFromBuff(buffer)

            if server_capabilities.has(CAPABILITIES.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) \
                    and capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA:
                self.enc_password = Datum('string<lenenc>')
                buffer = self.enc_password.setFromBuff(buffer)
            elif server_capabilities.has(CAPABILITIES.CLIENT_SECURE_CONNECTION) \
                    and capabilities.SECURE_CONNECTION:
                self.auth_resp_len = Datum('int<1>')
                buffer = self.auth_resp_len.setFromBuff(buffer)
                self.enc_password = Datum(f'string<{self.auth_resp_len.value}>')
                buffer = self.enc_password.setFromBuff(buffer)
            else:
                pass_byte = Datum('int<1>')
                buffer = pass_byte.setFromBuff(buffer)

            if capabilities.CONNECT_WITH_DB:
                buffer = self.database.setFromBuff(buffer)
            if capabilities.PLUGIN_AUTH:
                buffer = self.client_auth_plugin.setFromBuff(buffer)

            # at the end is CLIENT_CONNECT_ATTRS, but we dont use it and dont parse

        self.session.username = self.username.value

    def __str__(self):
        return str({
            'header': {'length': self.length, 'seq': self.seq},
            'username': self.username.value,
            'password': self.enc_password.value,
            'database': self.database.value
        })
