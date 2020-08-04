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

# import logging
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import COMMANDS, getConstName


class CommandPacket(Packet):
    '''
    Implementation based on description:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    def setup(self, length=0, count_header=1, body=''):
        if length == 0:
            return

        # self.salt=self.session.salt

        self._length = length
        self._seq = count_header
        self._body = body

        self.type = Datum('int<1>')
        buffer = body
        buffer = self.type.setFromBuff(buffer)

        if self.type.value in (COMMANDS.COM_QUERY, COMMANDS.COM_STMT_PREPARE):
            self.sql = Datum('str<EOF>')
            buffer = self.sql.setFromBuff(buffer)
        elif self.type.value == COMMANDS.COM_STMT_EXECUTE:
            self.stmt_id = Datum('int<4>')
            buffer = self.stmt_id.setFromBuff(buffer)
            self.flags = Datum('int<1>')
            buffer = self.flags.setFromBuff(buffer)
            self.iteration_count = Datum('int<4>')
            buffer = self.iteration_count.setFromBuff(buffer)
        elif self.type.value == COMMANDS.COM_STMT_CLOSE:
            self.stmt_id = Datum('int<4>')
            buffer = self.stmt_id.setFromBuff(buffer)
        elif self.type.value == COMMANDS.COM_STMT_FETCH:
            self.stmt_id = Datum('int<4>')
            buffer = self.stmt_id.setFromBuff(buffer)
            self.limit = Datum('int<4>')
            buffer = self.limit.setFromBuff(buffer)
        else:
            self.data = Datum('str<EOF>')
            buffer = self.data.setFromBuff(buffer)

    def __str__(self):
        return str({
            'header': {'length': self.length, 'seq': self.seq},
            'type': getConstName(COMMANDS, self.type.value),
            'vars': self.__dict__
        })
