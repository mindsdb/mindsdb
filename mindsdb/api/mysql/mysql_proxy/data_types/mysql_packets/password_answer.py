from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum


class PasswordAnswer(Packet):
    def setup(self, length=0, count_header=1, body=''):
        length = len(body)

        if length == 0:
            return
        self.password = Datum('string<NUL>')
        buffer = body
        buffer = self.password.setFromBuff(buffer)
