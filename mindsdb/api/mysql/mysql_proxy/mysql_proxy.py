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

import atexit
import base64
import os
import select
import socket
import socketserver as SocketServer
import ssl
import struct
import sys
import tempfile
import traceback
from functools import partial
from typing import List
from dataclasses import dataclass

from mindsdb.api.mysql.mysql_proxy.data_types.mysql_datum import Datum
import mindsdb.utilities.hooks as hooks
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.sql import clear_sql
from mindsdb.api.mysql.mysql_proxy.classes.client_capabilities import ClentCapabilities
from mindsdb.api.mysql.mysql_proxy.classes.server_capabilities import (
    server_capabilities,
)
from mindsdb.api.executor.controllers import SessionController
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packet import Packet
from mindsdb.api.mysql.mysql_proxy.data_types.mysql_packets import (
    BinaryResultsetRowPacket,
    ColumnCountPacket,
    ColumnDefenitionPacket,
    CommandPacket,
    EofPacket,
    ErrPacket,
    FastAuthFail,
    HandshakePacket,
    HandshakeResponsePacket,
    OkPacket,
    PasswordAnswer,
    ResultsetRowPacket,
    STMTPrepareHeaderPacket,
    SwitchOutPacket,
    SwitchOutResponse,
)
from mindsdb.api.mysql.mysql_proxy.executor import Executor
from mindsdb.api.mysql.mysql_proxy.external_libs.mysql_scramble import (
    scramble as scramble_func,
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import (
    DEFAULT_AUTH_METHOD,
    CHARSET_NUMBERS,
    SERVER_STATUS,
    CAPABILITIES,
    NULL_VALUE,
    COMMANDS,
    ERR,
    getConstName
)
from mindsdb.api.executor.data_types.answer import ExecuteAnswer
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.api.mysql.mysql_proxy.utilities import (
    ErWrongCharset,
    SqlApiException,
)
from mindsdb.api.executor import exceptions as exec_exc

from mindsdb.api.common.check_auth import check_auth
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.api.executor.sql_query.result_set import Column, ResultSet
from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.otel import increment_otel_query_request_counter
from mindsdb.utilities.wizards import make_ssl_cert
from mindsdb.api.mysql.mysql_proxy.utilities.dump import dump_result_set_to_mysql, column_to_mysql_column_dict

logger = log.getLogger(__name__)


def empty_fn():
    pass


@dataclass
class SQLAnswer:
    resp_type: RESPONSE_TYPE = RESPONSE_TYPE.OK
    result_set: ResultSet | None = None
    status: int | None = None
    state_track: List[List] | None = None
    error_code: int | None = None
    error_message: str | None = None
    affected_rows: int | None = None
    mysql_types: list[MYSQL_DATA_TYPE] | None = None

    @property
    def type(self):
        return self.resp_type

    def dump_http_response(self) -> dict:
        if self.resp_type == RESPONSE_TYPE.OK:
            return {
                "type": self.resp_type,
                "affected_rows": self.affected_rows,
            }
        elif self.resp_type in (RESPONSE_TYPE.TABLE, RESPONSE_TYPE.COLUMNS_TABLE):
            data = self.result_set.to_lists(json_types=True)
            return {
                "type": RESPONSE_TYPE.TABLE,
                "data": data,
                "column_names": [
                    column.alias or column.name
                    for column in self.result_set.columns
                ],
            }
        elif self.resp_type == RESPONSE_TYPE.ERROR:
            return {
                "type": RESPONSE_TYPE.ERROR,
                "error_code": self.error_code or 0,
                "error_message": self.error_message,
            }
        else:
            raise ValueError(f"Unsupported response type for dump HTTP response: {self.resp_type}")


class MysqlProxy(SocketServer.BaseRequestHandler):
    """
    The Main Server controller class
    """

    @staticmethod
    def server_close(srv):
        srv.server_close()

    def __init__(self, request, client_address, server):
        self.charset = "utf8"
        self.charset_text_type = CHARSET_NUMBERS["utf8_general_ci"]
        self.session = None
        self.client_capabilities = None
        self.connection_id = None
        super().__init__(request, client_address, server)

    def init_session(self):
        logger.debug(
            "New connection [{ip}:{port}]".format(
                ip=self.client_address[0], port=self.client_address[1]
            )
        )

        if self.server.connection_id >= 65025:
            self.server.connection_id = 0
        self.server.connection_id += 1
        self.connection_id = self.server.connection_id
        self.session = SessionController(api_type='sql')

        if hasattr(self.server, "salt") and isinstance(self.server.salt, str):
            self.salt = self.server.salt
        else:
            self.salt = base64.b64encode(os.urandom(15)).decode()

        self.socket = self.request
        self.logging = logger

        self.current_transaction = None

        logger.debug("session salt: {salt}".format(salt=self.salt))

    def handshake(self):
        def switch_auth(method="mysql_native_password"):
            self.packet(SwitchOutPacket, seed=self.salt, method=method).send()
            switch_out_answer = self.packet(SwitchOutResponse)
            switch_out_answer.get()
            password = switch_out_answer.password
            if method == "mysql_native_password" and len(password) == 0:
                password = scramble_func("", self.salt)
            return password

        def get_fast_auth_password():
            logger.debug("Asking for fast auth password")
            self.packet(FastAuthFail).send()
            password_answer = self.packet(PasswordAnswer)
            password_answer.get()
            try:
                password = password_answer.password.value.decode()
            except Exception:
                logger.warning("error: no password in Fast Auth answer")
                self.packet(
                    ErrPacket,
                    err_code=ERR.ER_PASSWORD_NO_MATCH,
                    msg="Is not password in connection query.",
                ).send()
                return None
            return password

        username = None
        password = None

        logger.debug("send HandshakePacket")
        self.packet(HandshakePacket).send()

        handshake_resp = self.packet(HandshakeResponsePacket)
        handshake_resp.get()
        if handshake_resp.length == 0:
            logger.debug("HandshakeResponsePacket empty")
            self.packet(OkPacket).send()
            return False
        self.client_capabilities = ClentCapabilities(handshake_resp.capabilities.value)

        client_auth_plugin = handshake_resp.client_auth_plugin.value.decode()

        self.session.is_ssl = False

        if handshake_resp.type == "SSLRequest":
            logger.debug("switch to SSL")
            self.session.is_ssl = True

            ssl_context = ssl.SSLContext()
            ssl_context.load_cert_chain(self.server.cert_path)
            ssl_socket = ssl_context.wrap_socket(
                self.socket, server_side=True, do_handshake_on_connect=True
            )

            self.socket = ssl_socket
            handshake_resp = self.packet(HandshakeResponsePacket)
            handshake_resp.get()
            client_auth_plugin = handshake_resp.client_auth_plugin.value.decode()

        username = handshake_resp.username.value.decode()

        if client_auth_plugin != DEFAULT_AUTH_METHOD:
            if client_auth_plugin == "mysql_native_password":
                password = switch_auth("mysql_native_password")
            else:
                new_method = (
                    "caching_sha2_password"
                    if client_auth_plugin == "caching_sha2_password"
                    else "mysql_native_password"
                )

                if (
                    new_method == "caching_sha2_password"
                    and self.session.is_ssl is False
                ):
                    logger.warning(
                        f"Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: "
                        "error: cant switch to caching_sha2_password without SSL"
                    )
                    self.packet(
                        ErrPacket,
                        err_code=ERR.ER_PASSWORD_NO_MATCH,
                        msg="caching_sha2_password without SSL not supported",
                    ).send()
                    return False

                logger.debug(
                    f"Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: "
                    f"switch auth method to {new_method}"
                )
                password = switch_auth(new_method)

                if new_method == "caching_sha2_password":
                    if password == b"\x00":
                        password = ""
                    else:
                        password = get_fast_auth_password()
        elif "caching_sha2_password" in client_auth_plugin:
            logger.debug(
                f"Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: "
                "check auth using caching_sha2_password"
            )
            password = handshake_resp.enc_password.value
            if password == b"\x00":
                password = ""
            else:
                # FIXME https://github.com/mindsdb/mindsdb/issues/1374
                # if self.session.is_ssl:
                #     password = get_fast_auth_password()
                # else:
                password = switch_auth()
        elif "mysql_native_password" in client_auth_plugin:
            logger.debug(
                f"Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: "
                "check auth using mysql_native_password"
            )
            password = handshake_resp.enc_password.value
        else:
            logger.debug(
                f"Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: "
                "unknown method, possible ERROR. Try to switch to mysql_native_password"
            )
            password = switch_auth("mysql_native_password")

        try:
            self.session.database = handshake_resp.database.value.decode()
        except Exception:
            self.session.database = None
        logger.debug(
            f"Check auth, user={username}, ssl={self.session.is_ssl}, auth_method={client_auth_plugin}: "
            f"connecting to database {self.session.database}"
        )

        auth_data = self.server.check_auth(
            username, password, scramble_func, self.salt, ctx.company_id
        )
        if auth_data["success"]:
            self.session.username = auth_data["username"]
            self.session.auth = True
            self.packet(OkPacket).send()
            return True
        else:
            self.packet(
                ErrPacket,
                err_code=ERR.ER_PASSWORD_NO_MATCH,
                msg=f"Access denied for user {username}",
            ).send()
            logger.warning(f"Access denied for user {username}")
            return False

    def send_package_group(self, packages):
        string = b"".join([x.accum() for x in packages])
        self.socket.sendall(string)

    def answer_stmt_close(self, stmt_id):
        self.session.unregister_stmt(stmt_id)

    def send_query_answer(self, answer: SQLAnswer):
        if answer.type in (RESPONSE_TYPE.TABLE, RESPONSE_TYPE.COLUMNS_TABLE):
            packages = []

            if len(answer.result_set) > 1000:
                # for big responses leverage pandas map function to convert data to packages
                self.send_table_packets(result_set=answer.result_set)
            else:
                packages += self.get_table_packets(result_set=answer.result_set)

            if answer.status is not None:
                packages.append(self.last_packet(status=answer.status))
            else:
                packages.append(self.last_packet())
            self.send_package_group(packages)
        elif answer.type == RESPONSE_TYPE.OK:
            self.packet(OkPacket, state_track=answer.state_track, affected_rows=answer.affected_rows).send()
        elif answer.type == RESPONSE_TYPE.ERROR:
            self.packet(
                ErrPacket, err_code=answer.error_code, msg=answer.error_message
            ).send()

    def _get_column_defenition_packets(self, columns: dict, data=None):
        if data is None:
            data = []
        packets = []
        for i, column in enumerate(columns):
            logger.debug(
                "%s._get_column_defenition_packets: handling column - %s of %s type",
                self.__class__.__name__,
                column,
                type(column),
            )
            table_name = column.get("table_name", "table_name")
            column_name = column.get("name", "column_name")
            column_alias = column.get("alias", column_name)
            flags = column.get("flags", 0)
            if isinstance(flags, list):
                flags = sum(flags)
            if column.get('size') is None:
                length = 1
                for row in data:
                    if isinstance(row, dict):
                        length = max(len(str(row[column_alias])), length)
                    else:
                        length = max(len(str(row[i])), length)
                column['size'] = 1

            packets.append(
                self.packet(
                    ColumnDefenitionPacket,
                    schema=column.get("database", "mindsdb_schema"),
                    table_alias=column.get("table_alias", table_name),
                    table_name=table_name,
                    column_alias=column_alias,
                    column_name=column_name,
                    column_type=column["type"],
                    charset=column.get("charset", CHARSET_NUMBERS["utf8_unicode_ci"]),
                    max_length=column["size"],
                    flags=flags,
                )
            )
        return packets

    def get_table_packets(self, result_set: ResultSet, status=0):
        data_frame, columns_dict = dump_result_set_to_mysql(result_set)
        data = data_frame.to_dict('split')['data']

        # TODO remove columns order
        packets = [self.packet(ColumnCountPacket, count=len(columns_dict))]
        packets.extend(self._get_column_defenition_packets(columns_dict, data))

        if self.client_capabilities.DEPRECATE_EOF is False:
            packets.append(self.packet(EofPacket, status=status))

        packets += [self.packet(ResultsetRowPacket, data=x) for x in data]
        return packets

    def send_table_packets(self, result_set: ResultSet, status: int = 0):
        df, columns_dicts = dump_result_set_to_mysql(result_set, infer_column_size=True)
        # text protocol, convert all to string and serialize as packages

        def apply_f(v):
            if v is None:
                return NULL_VALUE
            if not isinstance(v, str):
                v = str(v)
            return Datum.serialize_str(v)

        # columns packages
        packets = [self.packet(ColumnCountPacket, count=len(columns_dicts))]

        packets.extend(self._get_column_defenition_packets(columns_dicts))

        if self.client_capabilities.DEPRECATE_EOF is False:
            packets.append(self.packet(EofPacket, status=status))
        self.send_package_group(packets)

        chunk_size = 100
        for start in range(0, len(df), chunk_size):
            string = b"".join([
                self.packet(body=body, length=len(body)).accum()
                for body in df[start:start + chunk_size].applymap(apply_f).values.sum(axis=1)
            ])
            self.socket.sendall(string)

    def decode_utf(self, text):
        try:
            return text.decode("utf-8")
        except Exception:
            raise ErWrongCharset(f"SQL contains non utf-8 values: {text}")

    def is_cloud_connection(self):
        """Determine source of connection. Must be call before handshake.
        Idea based on: real mysql connection does not send anything before server handshake, so
        soket should be in 'out' state. In opposite, clout connection sends '0000' right after
        connection. '0000' selected because in real mysql connection it should be lenght of package,
        and it can not be 0.
        """
        is_cloud = config.get("cloud", False)

        if sys.platform != "linux" or is_cloud is False:
            return {"is_cloud": False}

        read_poller = select.poll()
        read_poller.register(self.request, select.POLLIN)
        events = read_poller.poll(30)

        if len(events) == 0:
            return {"is_cloud": False}

        first_byte = self.request.recv(4, socket.MSG_PEEK)
        if first_byte == b"\x00\x00\x00\x00":
            self.request.recv(4)
            client_capabilities = self.request.recv(8)
            client_capabilities = struct.unpack("L", client_capabilities)[0]

            company_id = self.request.recv(4)
            company_id = struct.unpack("I", company_id)[0]

            user_class = self.request.recv(1)
            user_class = struct.unpack("B", user_class)[0]
            email_confirmed = 1
            if user_class > 1:
                email_confirmed = (user_class >> 2) & 1
            user_class = user_class & 3

            database_name_len = self.request.recv(2)
            database_name_len = struct.unpack("H", database_name_len)[0]

            database_name = ""
            if database_name_len > 0:
                database_name = self.request.recv(database_name_len).decode()

            return {
                "is_cloud": True,
                "client_capabilities": client_capabilities,
                "company_id": company_id,
                "user_class": user_class,
                "database": database_name,
                "email_confirmed": email_confirmed,
            }

        return {"is_cloud": False}

    def to_mysql_columns(self, columns_list: list[Column]) -> list[dict[str, str | int]]:
        database_name = None if self.session.database == "" else self.session.database.lower()
        return [column_to_mysql_column_dict(column, database_name=database_name) for column in columns_list]

    @profiler.profile()
    def process_query(self, sql) -> SQLAnswer:
        executor = Executor(session=self.session, sqlserver=self)
        executor.query_execute(sql)
        executor_answer = executor.executor_answer

        if executor_answer.data is None:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.OK,
                state_track=executor_answer.state_track,
                affected_rows=executor_answer.affected_rows
            )
        else:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.TABLE,
                state_track=executor_answer.state_track,
                result_set=executor_answer.data,
                status=executor.server_status,
                affected_rows=executor_answer.affected_rows,
                mysql_types=executor_answer.data.mysql_types
            )

        # Increment the counter and include metadata in attributes
        increment_otel_query_request_counter(ctx.get_metadata(query=sql))

        return resp

    def answer_stmt_prepare(self, sql):
        executor = Executor(session=self.session, sqlserver=self)
        stmt_id = self.session.register_stmt(executor)

        executor.stmt_prepare(sql)

        packages = [
            self.packet(
                STMTPrepareHeaderPacket,
                stmt_id=stmt_id,
                num_columns=len(executor.columns),
                num_params=len(executor.params),
            )
        ]

        if len(executor.params) > 0:
            parameters_def = self.to_mysql_columns(executor.params)
            packages.extend(self._get_column_defenition_packets(parameters_def))
            if self.client_capabilities.DEPRECATE_EOF is False:
                status = sum([SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT])
                packages.append(self.packet(EofPacket, status=status))

        if len(executor.columns) > 0:
            columns_def = self.to_mysql_columns(executor.columns)
            packages.extend(self._get_column_defenition_packets(columns_def))

            if self.client_capabilities.DEPRECATE_EOF is False:
                status = sum([SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT])
                packages.append(self.packet(EofPacket, status=status))

        self.send_package_group(packages)

    def answer_stmt_execute(self, stmt_id, parameters):
        prepared_stmt = self.session.prepared_stmts[stmt_id]
        executor: Executor = prepared_stmt["statement"]

        executor.stmt_execute(parameters)

        executor_answer: ExecuteAnswer = executor.executor_answer

        if executor_answer.data is None:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.OK, state_track=executor_answer.state_track
            )
            return self.send_query_answer(resp)

        # TODO prepared_stmt['type'] == 'lock' is not used but it works
        result_set = executor_answer.data
        data_frame, columns_dict = dump_result_set_to_mysql(result_set)
        data = data_frame.to_dict('split')['data']

        packages = [self.packet(ColumnCountPacket, count=len(columns_dict))]
        packages.extend(self._get_column_defenition_packets(columns_dict))

        if self.client_capabilities.DEPRECATE_EOF is False:
            packages.append(self.packet(EofPacket, status=0x0062))

        # send all
        for row in data:
            packages.append(
                self.packet(BinaryResultsetRowPacket, data=row, columns=columns_dict)
            )

        server_status = executor.server_status or 0x0002
        packages.append(self.last_packet(status=server_status))
        prepared_stmt["fetched"] += len(data)

        return self.send_package_group(packages)

    def answer_stmt_fetch(self, stmt_id, limit):
        prepared_stmt = self.session.prepared_stmts[stmt_id]
        executor = prepared_stmt["statement"]
        fetched = prepared_stmt["fetched"]
        executor_answer: ExecuteAnswer = executor.executor_answer

        if executor_answer.data is None:
            resp = SQLAnswer(
                resp_type=RESPONSE_TYPE.OK, state_track=executor_answer.state_track
            )
            return self.send_query_answer(resp)

        packages = []
        columns = self.to_mysql_columns(executor_answer.data.columns)
        for row in executor_answer.data[fetched:limit].to_lists():
            packages.append(
                self.packet(BinaryResultsetRowPacket, data=row, columns=columns)
            )

        prepared_stmt["fetched"] += len(executor_answer.data[fetched:limit])

        if len(executor_answer.data) <= limit + fetched:
            status = sum(
                [
                    SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                    SERVER_STATUS.SERVER_STATUS_LAST_ROW_SENT,
                ]
            )
        else:
            status = sum(
                [
                    SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT,
                    SERVER_STATUS.SERVER_STATUS_CURSOR_EXISTS,
                ]
            )

        packages.append(self.last_packet(status=status))
        self.send_package_group(packages)

    def handle(self):
        """
        Handle new incoming connections
        :return:
        """
        ctx.set_default()

        self.server.hook_before_handle()

        logger.debug("handle new incoming connection")
        cloud_connection = self.is_cloud_connection()

        ctx.company_id = cloud_connection.get("company_id")

        self.init_session()
        if cloud_connection["is_cloud"] is False:
            if self.handshake() is False:
                return
        else:
            ctx.user_class = cloud_connection["user_class"]
            ctx.email_confirmed = cloud_connection["email_confirmed"]
            self.client_capabilities = ClentCapabilities(
                cloud_connection["client_capabilities"]
            )
            self.session.database = cloud_connection["database"]
            self.session.username = "cloud"
            self.session.auth = True

        while True:
            logger.debug("Got a new packet")
            p = self.packet(CommandPacket)

            try:
                success = p.get()
            except Exception:
                logger.error("Session closed, on packet read error")
                logger.error(traceback.format_exc())
                return

            if success is False:
                logger.debug("Session closed by client")
                return

            logger.debug(
                "Command TYPE: {type}".format(type=getConstName(COMMANDS, p.type.value))
            )

            command_names = {
                COMMANDS.COM_QUERY: "COM_QUERY",
                COMMANDS.COM_STMT_PREPARE: "COM_STMT_PREPARE",
                COMMANDS.COM_STMT_EXECUTE: "COM_STMT_EXECUTE",
                COMMANDS.COM_STMT_FETCH: "COM_STMT_FETCH",
                COMMANDS.COM_STMT_CLOSE: "COM_STMT_CLOSE",
                COMMANDS.COM_QUIT: "COM_QUIT",
                COMMANDS.COM_INIT_DB: "COM_INIT_DB",
                COMMANDS.COM_FIELD_LIST: "COM_FIELD_LIST",
            }

            command_name = command_names.get(p.type.value, f"UNKNOWN {p.type.value}")
            sql = None
            response = None
            error_type = None
            error_code = None
            error_text = None
            error_traceback = None

            try:
                if p.type.value == COMMANDS.COM_QUERY:
                    sql = self.decode_utf(p.sql.value)
                    sql = clear_sql(sql)
                    logger.debug(f'Incoming query: {sql}')
                    profiler.set_meta(
                        query=sql, api="mysql", environment=config.get("environment")
                    )
                    with profiler.Context("mysql_query_processing"):
                        response = self.process_query(sql)
                elif p.type.value == COMMANDS.COM_STMT_PREPARE:
                    sql = self.decode_utf(p.sql.value)
                    self.answer_stmt_prepare(sql)
                elif p.type.value == COMMANDS.COM_STMT_EXECUTE:
                    self.answer_stmt_execute(p.stmt_id.value, p.parameters)
                elif p.type.value == COMMANDS.COM_STMT_FETCH:
                    self.answer_stmt_fetch(p.stmt_id.value, p.limit.value)
                elif p.type.value == COMMANDS.COM_STMT_CLOSE:
                    self.answer_stmt_close(p.stmt_id.value)
                elif p.type.value == COMMANDS.COM_QUIT:
                    logger.debug("Session closed, on client disconnect")
                    self.session = None
                    break
                elif p.type.value == COMMANDS.COM_INIT_DB:
                    new_database = p.database.value.decode()

                    executor = Executor(session=self.session, sqlserver=self)
                    executor.change_default_db(new_database)

                    response = SQLAnswer(RESPONSE_TYPE.OK)
                elif p.type.value == COMMANDS.COM_FIELD_LIST:
                    # this command is deprecated, but console client still use it.
                    response = SQLAnswer(RESPONSE_TYPE.OK)
                elif p.type.value == COMMANDS.COM_STMT_RESET:
                    response = SQLAnswer(RESPONSE_TYPE.OK)
                else:
                    logger.warning("Command has no specific handler, return OK msg")
                    logger.debug(str(p))
                    # p.pprintPacket() TODO: Make a version of print packet
                    # that sends it to debug instead
                    response = SQLAnswer(RESPONSE_TYPE.OK)

            except SqlApiException as e:
                # classified error
                error_type = "expected"

                response = SQLAnswer(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_code=e.err_code,
                    error_message=str(e),
                )

            except exec_exc.ExecutorException as e:
                # unclassified
                error_type = "expected"

                if isinstance(e, exec_exc.NotSupportedYet):
                    error_code = ERR.ER_NOT_SUPPORTED_YET
                elif isinstance(e, exec_exc.KeyColumnDoesNotExist):
                    error_code = ERR.ER_KEY_COLUMN_DOES_NOT_EXIST
                elif isinstance(e, exec_exc.TableNotExistError):
                    error_code = ERR.ER_TABLE_EXISTS_ERROR
                elif isinstance(e, exec_exc.WrongArgumentError):
                    error_code = ERR.ER_WRONG_ARGUMENTS
                elif isinstance(e, exec_exc.LogicError):
                    error_code = ERR.ER_WRONG_USAGE
                elif isinstance(e, (exec_exc.BadDbError, exec_exc.BadTableError)):
                    error_code = ERR.ER_BAD_DB_ERROR
                else:
                    error_code = ERR.ER_SYNTAX_ERROR

                response = SQLAnswer(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_code=error_code,
                    error_message=str(e),
                )
            except exec_exc.UnknownError as e:
                # unclassified
                error_type = "unexpected"

                response = SQLAnswer(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_code=ERR.ER_UNKNOWN_ERROR,
                    error_message=str(e),
                )

            except Exception as e:
                # any other exception
                error_type = "unexpected"
                error_traceback = traceback.format_exc()
                logger.error(
                    f"ERROR while executing query\n" f"{error_traceback}\n" f"{e}"
                )
                error_code = ERR.ER_SYNTAX_ERROR
                response = SQLAnswer(
                    resp_type=RESPONSE_TYPE.ERROR,
                    error_code=error_code,
                    error_message=str(e),
                )

            if response is not None:
                self.send_query_answer(response)
                if response.type == RESPONSE_TYPE.ERROR:
                    error_text = response.error_message
                    error_code = response.error_code
                    error_type = error_type or "expected"

            hooks.after_api_query(
                company_id=ctx.company_id,
                api="mysql",
                command=command_name,
                payload=sql,
                error_type=error_type,
                error_code=error_code,
                error_text=error_text,
                traceback=error_traceback,
            )

    def packet(self, packetClass=Packet, **kwargs):
        """
        Factory method for packets

        :param packetClass:
        :param kwargs:
        :return:
        """
        p = packetClass(socket=self.socket, session=self.session, proxy=self, **kwargs)
        self.session.inc_packet_sequence_number()
        return p

    def last_packet(self, status=0x0002):
        if self.client_capabilities.DEPRECATE_EOF is True:
            return self.packet(OkPacket, eof=True, status=status)
        else:
            return self.packet(EofPacket, status=status)

    def set_context(self, context):
        if "db" in context:
            self.session.database = context["db"]
        else:
            self.session.database = config.get('default_project')

        if "profiling" in context:
            self.session.profiling = context["profiling"]
        if "predictor_cache" in context:
            self.session.predictor_cache = context["predictor_cache"]
        if "show_secrets" in context:
            self.session.show_secrets = context["show_secrets"]

    def get_context(self):
        context = {
            "show_secrets": self.session.show_secrets
        }
        if self.session.database is not None:
            context["db"] = self.session.database
        if self.session.profiling is True:
            context["profiling"] = True
        if self.session.predictor_cache is False:
            context["predictor_cache"] = False

        return context

    @staticmethod
    def startProxy():
        """
        Create a server and wait for incoming connections until Ctrl-C
        """
        global logger

        cert_path = config["api"]["mysql"].get("certificate_path")
        if cert_path is None or cert_path == "":
            cert_path = tempfile.mkstemp(prefix="mindsdb_cert_", text=True)[1]
            make_ssl_cert(cert_path)
            atexit.register(lambda: os.remove(cert_path))
        elif not os.path.exists(cert_path):
            logger.error("Certificate defined in 'certificate_path' setting does not exist")

        # TODO make it session local
        server_capabilities.set(CAPABILITIES.CLIENT_SSL, config["api"]["mysql"]["ssl"])

        host = config["api"]["mysql"]["host"]
        port = int(config["api"]["mysql"]["port"])

        logger.info(f"Starting MindsDB Mysql proxy server on tcp://{host}:{port}")

        SocketServer.TCPServer.allow_reuse_address = True
        server = SocketServer.ThreadingTCPServer((host, port), MysqlProxy)
        server.mindsdb_config = config
        server.check_auth = partial(check_auth, config=config)
        server.cert_path = cert_path
        server.connection_id = 0
        server.hook_before_handle = empty_fn

        atexit.register(MysqlProxy.server_close, srv=server)

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        logger.info("Waiting for incoming connections...")
        server.serve_forever()
