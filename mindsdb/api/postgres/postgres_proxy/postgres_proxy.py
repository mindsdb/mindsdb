import base64
import os
import socketserver
from typing import Callable, Dict, Type, Any, Iterable, Sequence

from mindsdb.api.mysql.mysql_proxy.controllers import SessionController
from mindsdb.api.mysql.mysql_proxy.executor import Executor
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CHARSET_NUMBERS
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import IntField, GenericField, PostgresField
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_formats import Terminate, \
    Query, NoticeResponse, AuthenticationClearTextPassword, AuthenticationOk, RowDescriptions, DataRow, CommandComplete, \
    ReadyForQuery
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message import PostgresMessage
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_packets import PostgresPacketReader
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log


class PostgresProxyHandler(socketserver.StreamRequestHandler):
    client_buffer: PostgresPacketReader

    def __init__(self, request, client_address, server):
        self.logger = get_log("postgres_proxy")
        self.charset = 'utf8'
        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.session = None
        self.client_capabilities = None
        super().__init__(request, client_address, server)

    def handle(self) -> None:

        ctx.set_default()
        self.init_session()
        self.message_map: Dict[Type[PostgresMessage], Callable[[Any], bool]] = {
            Terminate: self.terminate,
            Query: self.query
        }
        self.client_buffer = PostgresPacketReader(self.rfile)
        self.start_connection()
        self.main_loop()

    def init_session(self):
        self.logger.debug('New connection [{ip}:{port}]'.format(
            ip=self.client_address[0], port=self.client_address[1]))
        self.logger.debug(self.__dict__)

        if self.server.connection_id >= 65025:
            self.server.connection_id = 0
        self.server.connection_id += 1
        self.connection_id = self.server.connection_id
        self.session = SessionController()
        self.session.database = 'mindsdb'

        if hasattr(self.server, 'salt') and isinstance(self.server.salt, str):
            self.salt = self.server.salt
        else:
            self.salt = base64.b64encode(os.urandom(15)).decode()

        self.socket = self.request

        self.current_transaction = None

        self.logger.debug('session salt: {salt}'.format(salt=self.salt))

    def process_query(self, sql):
        executor = Executor(
            session=self.session,
            sqlserver=self
        )

        executor.query_execute(sql)
        if executor.data is None:
            self.logger.info(executor.state_track)
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.OK,
                state_track=executor.state_track,
            )
        else:
            self.logger.info(executor.state_track)
            self.logger.info(executor.data)
            self.logger.info(executor.columns)
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.TABLE,
                state_track=executor.state_track,
                columns=executor.to_mysql_columns(executor.columns),
                data=executor.data,
                status=executor.server_status
            )
        return resp

    def start_connection(self):
        self.handshake()
        self.authenticate()

    def send(self, message: PostgresMessage):
        message.send(self.wfile)

    def handshake(self):
        self.client_buffer.read_verify_ssl_request()
        self.send(NoticeResponse())
        self.client_buffer.read_startup_message()

    def authenticate(self):
        self.send(AuthenticationClearTextPassword())
        self.client_buffer.read_authentication()
        self.send(AuthenticationOk())

    def terminate(self, message: Terminate) -> bool:
        return False

    def query(self, message: Query) -> bool:
        self.logger.debug("Got query of:\n%s" % message.sql)
        result = self.process_query(message.get_parsed_sql())
        fields = self.to_postgres_fields(result.columns)
        # Returning sample data
        # fields = [IntField('first'), IntField('second')]
        # rows = [[b'5', b'6'], [b'1', b'2'], [b'9', b'8'], [b'11', b'23131']]
        rows = self.to_postgres_rows(result.data)
        self.send(RowDescriptions(fields=fields))
        self.send(DataRow(rows=rows))
        self.send(CommandComplete(tag=b'SELECT'))
        return True

    @staticmethod
    def to_postgres_fields(columns: Iterable[Dict[str, Any]]) -> Sequence[PostgresField]:
        fields = []
        i = 0
        for column in columns:
            fields.append(GenericField(
                name=column['name'],
                object_id=column['type'],
                column_id=i
            ))
            i += 1
        return fields

    def to_postgres_rows(self, rows: Iterable[Iterable[Any]]) -> Sequence[Sequence[bytes]]:
        p_rows = []
        for row in rows:
            p_row = []
            for column in row:
                if type(column) == int or type(column) == float:
                    column = str(column)
                p_row.append(column.encode(encoding=self.charset))
            p_rows.append(p_row)
        return p_rows

    def main_loop(self):
        while True:
            self.send(ReadyForQuery())
            message: PostgresMessage = self.client_buffer.read_message()
            tof = type(message)
            if tof in self.message_map:
                res = self.message_map[tof](message)
                if not res:
                    break
            else:
                self.logger.warning("Ignoring unsupported message type %s" % tof)

    @staticmethod
    def startProxy():
        config = Config()

        server = TcpServer(("localhost", 55432), PostgresProxyHandler)
        server.connection_id = 0
        server.mindsdb_config = config
        try:
            server.serve_forever()
        except:
            server.shutdown()


class TcpServer(socketserver.TCPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    PostgresProxyHandler.startProxy()
