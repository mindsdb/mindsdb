import socketserver
from typing import Callable, Dict, Type

from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import IntField
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_formats import Terminate, \
    Query, NoticeResponse, AuthenticationClearTextPassword, AuthenticationOk, RowDescriptions, DataRow, CommandComplete, \
    ReadyForQuery
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message import PostgresMessage
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_packets import PostgresPacketReader

from mindsdb.utilities.log import get_log


class PostgresProxyHandler(socketserver.StreamRequestHandler):
    client_buffer: PostgresPacketReader

    def handle(self) -> None:
        self.logger = get_log("postgres_proxy")
        self.message_map: Dict[Type[PostgresMessage], Callable[[Any], bool]] = {
            Terminate: self.terminate,
            Query: self.query
        }
        self.client_buffer = PostgresPacketReader(self.rfile)
        self.start_connection()
        self.main_loop()

    def start_connection(self):
        self.handshake()
        print("handshook")
        self.authenticate()
        print("authed")
    def send(self, message: PostgresMessage):
        message.send(self.wfile)

    def handshake(self):
        self.client_buffer.read_verify_ssl_request()
        print("read ssl")
        self.send(NoticeResponse())
        print("sent notice")
        self.client_buffer.read_startup_message()
        print("read startup")

    def authenticate(self):
        self.send(AuthenticationClearTextPassword())
        print("sent request for password")
        self.client_buffer.read_authentication()
        print("read authentication")
        self.send(AuthenticationOk())
        print("told you it was okay")

    def terminate(self, message: Terminate) -> bool:
        return False

    def query(self, message: Query) -> bool:
        print("got query")
        self.logger.debug("Got query of:\n%s" % message.sql)
        # Returning sample data
        fields = [IntField('first'), IntField('second')]
        rows = [[b'5', b'6'], [b'1', b'2'], [b'9', b'8'], [b'11', b'23131']]
        print(rows)
        self.send(RowDescriptions(fields=fields))
        self.send(DataRow(rows=rows))
        self.send(CommandComplete(tag=b'SELECT'))
        return True

    def main_loop(self):
        while True:
            self.send(ReadyForQuery())
            message: PostgresMessage = self.client_buffer.read_message()
            print(message)
            tof = type(message)
            if tof in self.message_map:
                res = self.message_map[tof](message)
                if not res:
                    break
            else:
                self.logger.warning("Ignoring unsupported message type %s" % tof)


class TcpServer(socketserver.TCPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    server = TcpServer(("localhost", 55432), PostgresProxyHandler)
    try:
        server.serve_forever()
    except:
        server.shutdown()
