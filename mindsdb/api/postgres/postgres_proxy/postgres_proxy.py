import base64
import os
import socketserver
from functools import partial
from typing import Callable, Dict, Type, Any, Iterable, Sequence

from mindsdb.api.common.controllers import SessionController
from mindsdb.api.common.executor import Executor
from mindsdb.api.common.libs import CHARSET_NUMBERS
from mindsdb.api.common.libs import RESPONSE_TYPE
from mindsdb.api.common.check_auth import check_auth
from mindsdb.api.common.classes.sql_answer import SQLAnswer
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import GenericField, PostgresField
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_formats import Terminate, \
    Query, NoticeResponse, AuthenticationClearTextPassword, AuthenticationOk, RowDescriptions, DataRow, CommandComplete, \
    ReadyForQuery, ConnectionFailure, ParameterStatus
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message import PostgresMessage
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_packets import PostgresPacketReader
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log
from mindsdb.api.common.external_libs.mysql_scramble import scramble as scramble_func

class PostgresProxyHandler(socketserver.StreamRequestHandler):
    client_buffer: PostgresPacketReader
    user_parameters: Dict[bytes, bytes]
    def __init__(self, request, client_address, server):
        self.logger = get_log("postgres_proxy")
        self.charset = 'utf8'
        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.session = None
        self.client_capabilities = None
        self.user_parameters = None
        super().__init__(request, client_address, server)

    def handle(self) -> None:

        ctx.set_default()
        self.init_session()
        self.message_map: Dict[Type[PostgresMessage], Callable[[Any], bool]] = {
            Terminate: self.terminate,
            Query: self.query
        }
        self.client_buffer = PostgresPacketReader(self.rfile)
        started = self.start_connection()
        if started:
            self.logger.debug("connection started")
            self.send_initial_data()
            self.main_loop()

    def init_session(self):
        self.logger.info('New connection [{ip}:{port}]'.format(
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
        self.logger.debug("processing query\n%s", sql)
        executor.query_execute(sql)
        if executor.data is None:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.OK,
                state_track=executor.state_track,
            )
        else:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.TABLE,
                state_track=executor.state_track,
                columns=executor.to_mysql_columns(executor.columns),
                data=executor.data,
                status=executor.server_status
            )
        return resp

    def start_connection(self):
        self.logger.debug("starting handshake")
        self.handshake()
        self.logger.debug("handshake complete, checking authentication")
        return self.authenticate()

    def send(self, message: PostgresMessage):
        message.send(self.wfile)

    def handshake(self):
        self.client_buffer.read_verify_ssl_request()
        self.send(NoticeResponse())
        self.user_parameters = self.client_buffer.read_startup_message()

    def authenticate(self, ask_for_password = False):
        if ask_for_password:
            self.send(AuthenticationClearTextPassword())
            password = self.client_buffer.read_authentication(encoding=self.charset)
        else:
            password = ''
        username = self.user_parameters[b'user'].decode(encoding=self.charset)
        auth_data = self.server.check_auth(username, password, scramble_func, self.salt, ctx.company_id)
        if auth_data['success']:
            self.logger.debug("Authentication succeeded")
            self.session.username = auth_data['username']
            self.session.auth = True
            self.send(AuthenticationOk())
            return True
        else:
            if not ask_for_password: #try asking for password
                return self.authenticate(ask_for_password=True)
            self.logger.debug("Authentication failed")
            self.send(ConnectionFailure(message="Authentication failed."))
            return False

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

    def send_initial_data(self):
        # TODO: Send BackendKeyData Here (55.2.1)
        self.send(ParameterStatus(name=b"server_version", value=b"14.6"))
        self.send(ParameterStatus(name=b"server_encoding", value=self.charset.encode(self.charset)))
        self.send(ParameterStatus(name=b"client_encoding", value=self.user_parameters[b'client_encoding']))
        # TODO Send Parameters to complete set on 55.2.7 Asynchronous Operations E.G. - At present there is a
        #  hard-wired set of parameters for which ParameterStatus will be generated: they are server_version,
        #  server_encoding, client_encoding, application_name, default_transaction_read_only, in_hot_standby,
        #  is_superuser, session_authorization, DateStyle, IntervalStyle, TimeZone, integer_datetimes,
        #  and standard_conforming_strings
        return

    def main_loop(self):
        while True:
            self.logger.debug("Ready for Query")
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
        server.check_auth = partial(check_auth, config=config)
        try:
            server.serve_forever()
        except:
            server.shutdown()


class TcpServer(socketserver.TCPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    PostgresProxyHandler.startProxy()
