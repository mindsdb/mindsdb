from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum

class SwitchOutResponse(Packet):
    def setup(self, length=0, count_header=1, body=''):
        length = len(body)

        if length == 0:
            self.password = b''
            return

        self.enc_password = Datum('string<EOF>') # 0x04
        buffer = body
        buffer = self.enc_password.setFromBuff(buffer)
        self.password = self.enc_password.value
