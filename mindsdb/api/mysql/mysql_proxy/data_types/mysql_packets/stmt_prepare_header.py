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


class STMTPrepareHeaderPacket(Packet):
    '''
    Implementation based on description:
    https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
    '''

    def setup(self):
        self.status = Datum('int<1>', 0)
        self.stmt_id = Datum('int<4>', self._kwargs.get('stmt_id', 1))
        self.num_columns = Datum('int<2>', self._kwargs.get('num_columns', 0))
        self.num_params = Datum('int<2>', self._kwargs.get('num_params', 0))
        self.filler = Datum('int<1>', 0)
        self.warning_count = Datum('int<2>', 0)

    @property
    def body(self):
        order = [
            'status',
            'stmt_id',
            'num_columns',
            'num_params',
            'filler',
            'warning_count'
        ]
        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body
