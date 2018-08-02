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


        self.ok_header = Datum('int<1>', 0)
        self.affected_rows = Datum('int<lenenc>', 0)
        self.last_insert_id = Datum('int<lenenc>', 0)
        self.server_status = Datum('int<2>', 0)
        self.warning_count = Datum('int<2>', 0)
        self.info = Datum('string<EOF>')



    @property
    def body(self):

        order = [
            'ok_header',
            'affected_rows',
            'last_insert_id',
            'server_status',
            'warning_count',
            'info'
        ]
        string = b''
        for key in order:
            section_pack = getattr(self,key).toStringPacket()
            string += section_pack


        self.setBody(string)
        return self._body

    @staticmethod
    def test():
        import pprint
        logging.basicConfig(level=10)
        pprint.pprint(str(OkPacket().getPacketString()))


# only run the test if this file is called from debugger
if __name__ == "__main__":
    OkPacket.test()



