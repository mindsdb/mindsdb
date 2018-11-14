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
from libs.helpers.logging import logging

from libs.data_types.mysql_datum import *
from libs.data_types.mysql_packet import Packet


class ColumnCountPacket(Packet):

    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    def setup(self):
        count = 0 if 'count' not in self.kwargs else self.kwargs['count']

        self.column_count = Datum('int<lenenc>', count)

    @property
    def body(self):

        order = [
            'column_count'
        ]

        string = ''
        for key in order:
            string += getattr(self,key).toStringPacket()

        self.setBody(string)
        return self._body

