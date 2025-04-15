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

import struct

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import SESSION_TRACK, SERVER_STATUS


class OkPacket(Packet):
    '''
    Implementation based on:
    https://mariadb.com/kb/en/library/1-connecting-connecting/#initial-handshake-packet
    '''

    '''
    int<1> 0x00 : OK_Packet header or (0xFE if CLIENT_DEPRECATE_EOF is set)
    int<lenenc> affected rows
    int<lenenc> last insert id
    int<2> server status
    int<2> warning count
    if session_tracking_supported (see CLIENT_SESSION_TRACK)
        string<lenenc> info
    if (status flags & SERVER_SESSION_STATE_CHANGED)
        string<lenenc> session state info
        string<lenenc> value of variable
    else
        string<EOF> info
    '''

    def setup(self):
        eof = self._kwargs.get('eof', False)
        self.ok_header = Datum('int<1>', 0xFE if eof is True else 0)
        self.affected_rows = Datum('int<lenenc>', self._kwargs.get('affected_rows') or 0)
        self.last_insert_id = Datum('int<lenenc>', 0)
        status = self._kwargs.get('status', 0x0002)
        self.server_status = Datum('int<2>', status)
        # Datum('int<2>', 0)
        self.warning_count = Datum('int<2>', 0)

        self.state_track = None
        state_track = self._kwargs.get('state_track')   # [[key: value]]
        if state_track is not None:
            accum = b''
            status = status | SERVER_STATUS.SERVER_SESSION_STATE_CHANGED
            self.server_status = Datum('int<2>', status)
            self.state_track = b''
            self.state_track += Datum('string<lenenc>', '').toStringPacket()  # 'info' - human readable status information
            for el in state_track:
                # NOTE at this moment just system variables
                name, value = el
                part = Datum('string<lenenc>', name).toStringPacket()
                part += Datum('string<lenenc>', value).toStringPacket()
                accum += struct.pack('i', SESSION_TRACK.SESSION_TRACK_SYSTEM_VARIABLES)[:1] + struct.pack('i', len(part))[:1] + part
                # self.state_track
                # self.state_track.append(Datum('string<lenenc>', '')
            accum = struct.pack('i', len(accum))[:1] + accum
            self.state_track += accum

        self.info = Datum('string<EOF>')

    @property
    def body(self):

        order = [
            'ok_header',
            'affected_rows',
            'last_insert_id',
            'server_status',
            'warning_count',
            'state_track',
            'info',
        ]
        string = b''
        for key in order:
            item = getattr(self, key)
            section_pack = b''
            if item is None:
                continue
            elif isinstance(item, bytes):
                section_pack = item
            else:
                section_pack = getattr(self, key).toStringPacket()
            string += section_pack

        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        pprint.pprint(str(OkPacket(state_track=[['character_set_client', 'utf8'], ['character_set_connection', 'utf8'], ['character_set_results', 'utf8']]).get_packet_string()))


# only run the test if this file is called from debugger
if __name__ == "__main__":
    OkPacket.test()
