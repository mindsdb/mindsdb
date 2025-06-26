import base64
import datetime
import os
import json
import select
import socketserver
import struct
import sys
from functools import partial
import socket
from typing import Callable, Dict, Type, Any, Iterable, Sequence

from mindsdb.api.executor.controllers import SessionController
from mindsdb.api.postgres.postgres_proxy.executor import Executor
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CHARSET_NUMBERS
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.common.check_auth import check_auth
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.postgres.postgres_proxy.postgres_packets.errors import POSTGRES_SYNTAX_ERROR_CODE
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import GenericField, PostgresField
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_formats import Terminate, \
    Query, AuthenticationClearTextPassword, AuthenticationOk, RowDescriptions, DataRow, CommandComplete, \
    ReadyForQuery, ConnectionFailure, ParameterStatus, Error, Execute, Bind, Parse, Sync, ParseComplete, \
    InvalidSQLStatementName, BindComplete, Describe, DataException, ParameterDescription
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message import PostgresMessage
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_packets import PostgresPacketReader, \
    PostgresPacketBuilder
from mindsdb.api.postgres.postgres_proxy.utilities import strip_null_byte
from mindsdb.utilities.config import config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log
from mindsdb.api.mysql.mysql_proxy.external_libs.mysql_scramble import scramble as scramble_func


class PostgresProxyHandler(socketserver.StreamRequestHandler):
    client_buffer: PostgresPacketReader
    user_parameters: Dict[bytes, bytes]

    def __init__(self, request, client_address, server):
        self.logger = log.getLogger(__name__)
        self.charset = 'utf8'
        self.charset_text_type = CHARSET_NUMBERS['utf8_general_ci']
        self.session = None
        self.client_capabilities = None
        self.user_parameters = None
        self.named_statements = {}
        self.unnamed_statement = None
        self.is_cloud = False
        self.named_portals = {}
        self.unnamed_portal = None
        self.transaction_status = b'I'  # I: Idle, T: Transaction Block, E: Failed Transaction Block
        super().__init__(request, client_address, server)

    def handle(self) -> None:

        ctx.set_default()
        self.init_session()
        self.logger.debug('handle new incoming connection')
        cloud_connection = self.is_cloud_connection()
        if cloud_connection["is_cloud"]:
            ctx.company_id = cloud_connection.get('company_id')
            self.is_cloud = True

        self.message_map: Dict[Type[PostgresMessage], Callable[[Any], bool]] = {
            Terminate: self.terminate,
            Query: self.query,
            Parse: self.parse,
            Bind: self.bind,
            Execute: self.execute,
            Describe: self.describe,
            Sync: self.sync
        }
        self.client_buffer = PostgresPacketReader(self.rfile)
        if self.is_cloud:
            # We already have a connection started through the gateway.
            started = True
            self.handshake()
        else:
            started = self.start_connection()
        if started:
            self.logger.debug("connection started")
            self.send_initial_data()
            self.main_loop()

    def is_cloud_connection(self):
        """ Determine source of connection. Must be call before handshake.
                Idea based on: real mysql connection does not send anything before server handshake, so
                socket should be in 'out' state. In opposite, clout connection sends '0000' right after
                connection. '0000' selected because in real mysql connection it should be length of package,
                and it can not be 0.

                Copied from mysql_proxy
                TODO: Extract method into common
            """
        is_cloud = config.get('cloud', False)

        if sys.platform != 'linux' or is_cloud is False:
            return {
                'is_cloud': False
            }

        read_poller = select.poll()
        read_poller.register(self.request, select.POLLIN)
        events = read_poller.poll(30)

        if len(events) == 0:
            return {
                'is_cloud': False
            }

        first_byte = self.request.recv(4, socket.MSG_PEEK)
        if first_byte == b'\x00\x00\x00\x00':
            self.request.recv(4)
            client_capabilities = self.request.recv(8)
            client_capabilities = struct.unpack('L', client_capabilities)[0]

            company_id = self.request.recv(4)
            company_id = struct.unpack('I', company_id)[0]

            user_class = self.request.recv(1)
            user_class = struct.unpack('B', user_class)[0]

            database_name_len = self.request.recv(2)
            database_name_len = struct.unpack('H', database_name_len)[0]

            database_name = ''
            if database_name_len > 0:
                database_name = self.request.recv(database_name_len).decode()

            return {
                'is_cloud': True,
                'client_capabilities': client_capabilities,
                'company_id': company_id,
                'user_class': user_class,
                'database': database_name
            }

        return {
            'is_cloud': False
        }

    def parse(self, message: Parse):
        self.logger.info("Postgres_Proxy: Parsing")
        executor = Executor(
            session=self.session,
            proxy_server=self,
            charset=self.charset
        )
        # TODO: Remove comment if unneeded ot use session since we're storing in this proxy class per session anyway
        # stmt_id = self.session.register_stmt(executor)
        executor.stmt_prepare(sql=message.query)
        statement = {"executor": executor, "parse": message}
        if message.name:
            self.named_statements[message.name] = statement
        else:
            self.unnamed_statement = statement
        self.send(ParseComplete())
        return True

    def bind(self, message: Bind):
        self.logger.info(f"Postgres_Proxy: Binding {message.name} with params {message.parameters}")
        if message.statement_name:
            statement = self.named_statements[message.statement_name]
        elif self.unnamed_statement:
            statement = self.unnamed_statement
        else:
            self.send(InvalidSQLStatementName())
            return True

        # TODO Should check validity of statement here and not at parse stage
        portal = statement.copy()
        portal["bind"] = message
        if message.name:
            self.named_portals[message.name] = portal
        else:
            self.unnamed_portal = portal
        self.send(BindComplete())
        return True

    def describe(self, message: Describe):
        self.logger.info("Postgres_Proxy: Describing")
        if message.describe_type == b'P':
            if message.name:
                describing = self.named_portals[message.name]
            elif self.unnamed_statement:
                describing = self.unnamed_portal
            else:
                self.send(InvalidSQLStatementName("Portal Does not Exist"))
                return True
        elif message.describe_type == b'S':
            if message.name:
                describing = self.named_statements[message.name]
            elif self.unnamed_statement:
                describing = self.unnamed_statement
            else:
                self.send(InvalidSQLStatementName())
                return True
            self.send(ParameterDescription(parameters=describing["parse"]["parameters"]))
        else:
            self.send(DataException(message="Describe did not have correct type. Can be 'P' or 'S'"))
            return True

        fields = self.to_postgres_fields(describing["executor"].columns)
        self.send(RowDescriptions(fields=fields))
        return True

    def execute(self, message: Execute):
        self.logger.info("Postgres_Proxy: Executing")
        if message.name:
            portal = self.named_portals[message.name]
        elif self.unnamed_portal:
            portal = self.unnamed_portal
        else:
            self.send(InvalidSQLStatementName("Portal does not exist"))

        executor = portal["executor"]
        params = portal["bind"].parameters
        executor.stmt_execute(param_values=params)
        sql_answer = self.return_executor_data(executor)
        self.respond_from_sql_answer(sql=executor.sql, sql_answer=sql_answer, row_descs=False)
        return True

    def sync(self, message: Sync):
        self.logger.info("Postgres_Proxy: Syncing")
        # TODO: Close/commit transaction if outside of a block. Maybe no collaries since Proxy
        self.send_ready()
        return True

    def init_session(self):
        self.logger.info('New connection [{ip}:{port}]'.format(
            ip=self.client_address[0], port=self.client_address[1]))
        self.logger.debug(self.__dict__)

        if self.server.connection_id >= 65025:
            self.server.connection_id = 0
        self.server.connection_id += 1
        self.connection_id = self.server.connection_id
        self.session = SessionController()
        self.session.database = config.get('default_project')

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
            proxy_server=self,
            charset=self.charset
        )
        self.logger.debug("processing query\n%s", sql)
        try:
            executor.query_execute(sql)
        except Exception as e:
            return SQLAnswer(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=str(e).encode(self.get_encoding()),
                error_code=POSTGRES_SYNTAX_ERROR_CODE.encode(self.get_encoding())
            )
        return self.return_executor_data(executor)

    def return_executor_data(self, executor):
        if executor.data is None:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.OK,
                state_track=executor.state_track,
            )
        else:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.TABLE,
                state_track=executor.state_track,
                result_set=executor.data,
                status=executor.server_status
            )
        return resp

    def start_connection(self):
        self.logger.debug("starting handshake")
        self.handshake()
        self.logger.debug("handshake complete, checking authentication")
        return self.authenticate()

    def send(self, message: PostgresMessage):
        self.logger.debug("Sending message of type %s" % message.__class__.__name__)
        message.send(self.wfile)

    def handshake(self):
        self.client_buffer.read_verify_ssl_request()
        # self.send(NoticeResponse()) -- Should Probably not send. Looks in protocol manual to be sent for warning
        self.logger.debug("Sending No to SSL Request")
        PostgresPacketBuilder().write_char(b'N', self.wfile)
        self.user_parameters = self.client_buffer.read_startup_message()

    def authenticate(self, ask_for_password=False):
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
            if not ask_for_password:  # try asking for password
                return self.authenticate(ask_for_password=True)
            self.logger.debug("Authentication failed")
            self.send(ConnectionFailure(message="Authentication failed."))
            return False

    def terminate(self, message: Terminate) -> bool:
        self.logger.info("Postgres_Proxy: Terminating")
        return False

    def get_encoding(self) -> str:
        return self.user_parameters.get(b"user_encoding", None) or self.charset

    def return_ok(self, sql, rows: int = 0):
        command = self.get_command(sql)
        if command == "BEGIN":
            self.transaction_status = b'T'
        if command == "COMMIT":
            self.transaction_status = b'I'
        if command == "CREATE":
            command = "SELECT"
        if command in ("INSERT", "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY"):
            command = f"{command} {rows}"
        self.send(CommandComplete(tag=command.encode(encoding=self.get_encoding())))
        return True

    def get_command(self, sql):
        sql = self.stringify_sql(sql)
        return sql.split(" ", 1)[0]

    def stringify_sql(self, sql):
        if type(sql) == bytes:
            encoding = self.get_encoding()
            sql: str = sql.decode(encoding)
        return strip_null_byte(sql).strip(';')

    def return_table(self, sql_answer: SQLAnswer, row_descs=True):
        fields = self.to_postgres_fields(sql_answer.result_set.columns)
        rows = self.to_postgres_rows(sql_answer.result_set)
        if row_descs:
            self.send(RowDescriptions(fields=fields))
        self.send(DataRow(rows=rows))
        encoding = self.get_encoding()
        tag = ('SELECT %s' % str(len(rows))).encode(encoding)
        self.send(CommandComplete(tag=tag))
        return True

    def return_error(self, sql_answer: SQLAnswer):
        self.send(Error.from_answer(error_code=sql_answer.error_code, error_message=sql_answer.error_message))
        return True

    def query(self, message: Query) -> bool:
        self.logger.debug("Postgres Proxy: Got query of:\n%s" % message.sql)
        sql = message.get_parsed_sql()
        sql_answer = self.process_query(sql)
        self.respond_from_sql_answer(sql=sql, sql_answer=sql_answer)
        self.send_ready()
        return True

    def respond_from_sql_answer(self, sql, sql_answer: SQLAnswer, row_descs=True) -> bool:
        # TODO Add command complete passthrough for Complex Queries that exceed row limit in one go
        rows = 0
        if sql_answer.result_set:
            rows = len(sql_answer.result_set)
        if RESPONSE_TYPE.OK == sql_answer.type:
            return self.return_ok(sql, rows=rows)
        elif RESPONSE_TYPE.TABLE == sql_answer.type:
            return self.return_table(sql_answer, row_descs=row_descs)
        elif RESPONSE_TYPE.ERROR == sql_answer.type:
            return self.return_error(sql_answer)

    @staticmethod
    def to_postgres_fields(columns: Iterable[Dict[str, Any]]) -> Sequence[PostgresField]:
        fields = []
        i = 0
        for column in columns:
            fields.append(GenericField(
                name=column['name'],
                object_id=column['type'].value,
                column_id=i
            ))
            i += 1
        return fields

    def to_postgres_rows(self, rows: Iterable[Iterable[Any]]) -> Sequence[Sequence[bytes]]:
        p_rows = []
        for row in rows:
            p_row = []
            for column in row:
                if column is None:
                    column = ""
                elif type(column) == int or type(column) == float:
                    column = str(column)
                elif type(column) == list or type(column) == dict:
                    column = json.dumps(column)
                if isinstance(column, datetime.date) or isinstance(column, datetime.datetime):
                    try:
                        column = datetime.datetime.strftime(column, '%Y-%m-%d')
                    except ValueError:
                        try:
                            column = datetime.datetime.strftime(column, '%Y-%m-%dT%H:%M:%S')
                        except ValueError:
                            column = datetime.datetime.strptime(column, '%Y-%m-%dT%H:%M:%S.%f')
                if isinstance(column, bool):
                    if column:
                        column = "true"
                    else:
                        column = "false"
                p_row.append(column.encode(encoding=self.charset))
            p_rows.append(p_row)
        return p_rows

    def send_initial_data(self):
        server_encoding = self.charset.encode(self.charset)
        client_encoding = self.user_parameters.get(b'client_encoding', server_encoding)
        # TODO: Send BackendKeyData Here (55.2.1)
        self.send(ParameterStatus(name=b"server_version", value=b"14.6"))
        self.send(ParameterStatus(name=b"server_encoding", value=server_encoding))
        self.send(ParameterStatus(name=b"client_encoding", value=client_encoding))
        # TODO Send Parameters to complete set on 55.2.7 Asynchronous Operations E.G. - At present there is a
        #  hard-wired set of parameters for which ParameterStatus will be generated: they are server_version,
        #  server_encoding, client_encoding, application_name, default_transaction_read_only, in_hot_standby,
        #  is_superuser, session_authorization, DateStyle, IntervalStyle, TimeZone, integer_datetimes,
        #  and standard_conforming_strings
        return

    def send_ready(self):
        self.logger.debug("Ready for Query")
        self.send(ReadyForQuery(transaction_status=self.transaction_status))

    def main_loop(self):
        self.send_ready()
        while True:
            message: PostgresMessage = self.client_buffer.read_message()
            if message is None:  # Empty Data, Buffer done
                break
            tof = type(message)
            if tof in self.message_map:
                res = self.message_map[tof](message)
                if not res:
                    break
            else:
                self.logger.warning("Ignoring unsupported message type %s" % tof)

    @staticmethod
    def startProxy():
        host = config['api']['postgres']['host']
        port = int(config['api']['postgres']['port'])
        server = TcpServer((host, port), PostgresProxyHandler)
        server.connection_id = 0
        server.mindsdb_config = config
        server.check_auth = partial(check_auth, config=config)
        server.serve_forever()


class TcpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    PostgresProxyHandler.startProxy()
