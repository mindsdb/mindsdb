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
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CHARSET_NUMBERS, TYPES
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum


class ColumnDefenitionPacket(Packet):
    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/resultset/
    '''

    # https://dev.mysql.com/doc/internals/en/com-query-response.html
    def setup(self):
        self.catalog = Datum('string<lenenc>', 'def')
        self.schema = Datum(
            'string<lenenc>',
            self._kwargs.get('schema', ''))
        self.table_alias = Datum(
            'string<lenenc>',
            self._kwargs.get('table_alias', '')
        )
        self.table_name = Datum(
            'string<lenenc>',
            self._kwargs.get('table_name', '')
        )
        self.column_alias = Datum(
            'string<lenenc>',
            self._kwargs.get('column_alias', '')
        )
        self.column_name = Datum(
            'string<lenenc>',
            self._kwargs.get('column_name', '')
        )
        self.fixed_length = Datum('int<lenenc>', 0xC)
        charset = self._kwargs.get('charset', CHARSET_NUMBERS["utf8_unicode_ci"])
        self.character_set = Datum('int<2>', charset)
        self.column_length = Datum('int<4>', self._kwargs.get('max_length', 0xf))  # may be this? https://books.google.ru/books?id=G2YqBS9CQ0AC&lpg=PP1&hl=ru&pg=PA428#v=onepage&q&f=false
        self.column_type = Datum(
            'int<1>',
            self._kwargs.get(
                'column_type',
                self._kwargs.get('column_type', TYPES.MYSQL_TYPE_VARCHAR)
            )
        )

        self.flags = Datum('int<2>', self._kwargs.get('flags', 0))
        self.decimals = Datum('int<1>', 0)

        self.unused = Datum('int<2>', 0)

    @property
    def body(self):
        order = [
            'catalog',
            'schema',
            'table_alias',
            'table_name',
            'column_alias',
            'column_name',
            'fixed_length',
            'character_set',
            'column_length',
            'column_type',
            'flags',
            'decimals',
            'unused'
        ]
        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(str(ColumnDefenitionPacket().get_packet_string()))


# only run the test if this file is called from debugger
if __name__ == "__main__":
    ColumnDefenitionPacket.test()
