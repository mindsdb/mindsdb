from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum

class FastAuthFail(Packet):
    def setup(self):
        self.cont = Datum('int<1>', 4) # 0x04

    @property
    def body(self):

        order = [
            'cont'
        ]
        string = b''
        for key in order:
            string += getattr(self, key).toStringPacket()

        self.setBody(string)
        return self._body
