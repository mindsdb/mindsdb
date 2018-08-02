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

from mindsdb.mindsdb_server.proxies.mysql.data_types.mysql_packet import Packet

from libs.constants.mysql import *
from .eof_packet import EofPacket


class ResultsetPacket(Packet):

    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/resultset/
    '''

    def setup(self):

        columns = [{}] if 'columns' not in self._kwargs else self._kwargs[
            'columns']

        column_count = len(columns)

        self.column_count = Datum('int<lenenc>', column_count)
        self.column_def_packets = []

        for column in columns:
            schema = 'def_schema' if 'schema' not in column else column[
                'schema']
            table_alias = 'def_t_alias' if 'table_alias' not in column else column[
                'table_alias']
            table = 'def_table' if 'table' not in column else column['table']
            column_alias = 'def_c_alias' if 'column_alias' not in column else column[
                'column_alias']
            column_str = 'def_column' if 'column' not in column else column[
                'column']

            column_data = [
                Datum('string<lenenc>', 'def'),
                Datum('string<lenenc>', schema),
                Datum('string<lenenc>', table_alias),
                Datum('string<lenenc>', table),
                Datum('string<lenenc>', column_alias),
                Datum('string<lenenc>', column_str),
                Datum('int<lenenc>', int('0xC', 0)),  # LENGTH OF FIXED FIELDS
                Datum('int<2>', CHARSET_NUMBERS[
                      "utf8_unicode_ci"]),  # Charset number
                # Max column size TODO: Figure out how to calculate this
                Datum('int<4>', 200),
                Datum('int<1>', MYSQL_TYPE_VARCHAR if 'field_type' not in column else column[
                      'field_type']),
                Datum('int<1>', 0 if 'field_detail_flag' not in column else column[
                      'field_detail_flag']),
                Datum('int<1>', 0),  # Decimals
                Datum('int<2>', 0)  # unused
            ]

            self.column_def_packets += [column_data]

        self.eof_more = Datum('string<packet>', EofPacket(parent_packet=self))
        self.eof = Datum('string<packet>', EofPacket(parent_packet=self))

    @property
    def body(self):

        order = [
            'column_count',
            'column_def_packets',
            'eof_more',
            'eof'
        ]
        string = b''
        for key in order:
            attr = getattr(self, key)
            if type(attr) == type([]):
                for item in attr:
                    if type(item) == type([]):
                        for subitem in item:
                            string += subitem.toStringPacket()
                    else:
                        string += item.toStringPacket()
            else:
                string += attr.toStringPacket()

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        logging.basicConfig(level=10)
        pprint.pprint(str(ResultsetPacket().getPacketString()))


# only run the test if this file is called from debugger
if __name__ == "__main__":
    ResultsetPacket.test()
