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

import traceback
from pprint import pformat

from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packet import Packet

from external_libs.mysql_scramble import scramble, scramble_323


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

        if length < 30:
            reserved_type = 'string<1>'
            enc_pass_type = 'string<NUL>'
            self.scramble_func = scramble_323
        else:
            reserved_type = 'string<28>'
            enc_pass_type = 'string<NUL>'
            self.scramble_func = scramble

        self.capabilities = Datum('int<4>')
        self.reserved = Datum(reserved_type)
        self.username = Datum('string<NUL>')
        self.enc_password = Datum(enc_pass_type)
        self.database = Datum('string<NUL>')

        buffer = body
        buffer = self.capabilities.setFromBuff(buffer)
        buffer = self.reserved.setFromBuff(buffer)
        buffer = self.username.setFromBuff(buffer)
        buffer = self.enc_password.setFromBuff(buffer)

        #if len(buffer) > 0:
        #    self.database.setFromBuff(buffer)



        self.session.username = self.username.value



    # TODO: Not sure if this belongs here, explore moving it to Session
    def isAuthOk(self, user, password):


        try:
            if user != self.username.value.decode('ascii'):
                self.session.logging.warning('Authentication FAIL: User match error')
                return False

            self.session.logging.debug('checking against password: {p}:'.format(p=password))

            orig_scramble = self.scramble_func(password, self.salt)
            self.session.logging.debug('scramble of true: {p}:'.format(
                p=pformat(self.enc_password.value)))
            self.session.logging.debug('scramble of recv: {p}:'.format(
                p=pformat(orig_scramble)))
            if orig_scramble in self.enc_password.value:
                self.session.logging.info('Authentication was sucessful')
                self.session.logging.info('Setting session user as {user}'.format(
                    user=self.username.value))
                self.session.username = self.username.value
                self.session.auth = True
                return True
            else:
                self.session.logging.info('Authentication FAIL: password match error')
                return False
        except:
            self.session.logging.error(traceback.format_exc())
            self.session.logging.error('failed to authenticate')

    def __str__(self):
        return str({
            'header': {'length': self.length, 'seq': self.seq},
            'username': self.username.value,
            'password': self.enc_password.value,
            'database': self.database.value
        })
