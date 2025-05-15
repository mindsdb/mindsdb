from typing import BinaryIO, Sequence, Dict, Type

from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import PostgresField
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message import PostgresMessage
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_identifiers import \
    PostgresBackendMessageIdentifier, PostgresFrontendMessageIdentifier, PostgresAuthType

from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_packets import PostgresPacketReader
from mindsdb.api.postgres.postgres_proxy.utilities import strip_null_byte
from mindsdb.utilities.sql import clear_sql


# All docstrings for Messages are taken from
# https://www.postgresql.org/docs/current/protocol-message-formats.html as of 2023-2-8 for Postgresql 15


class NoticeResponse(PostgresMessage):
    """
    NoticeResponse (B)
    Byte1('N')
    Identifies the message as a notice.

    Int32
    Length of message contents in bytes, including self.

    The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can
    appear in any order. For each field there is the following:

    Byte1
    A code identifying the field type; if zero, this is the message terminator and no string follows. The
    presently defined field types are listed in Section 55.8. Since more field types might be added in future,
    frontends should silently ignore fields of unrecognized type.

    String
    The field value. """  # noqa

    def __init__(self):
        self.identifier = PostgresBackendMessageIdentifier.NOTICE_RESPONSE
        self.backend_capable = True
        self.frontend_capable = False
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder().write(write_file=write_file)


class AuthenticationOk(PostgresMessage):
    """
    AuthenticationOk (B)
    Byte1('R')
    Identifies the message as an authentication request.

    Int32(8)
    Length of message contents in bytes, including self.

    Int32(0)
    Specifies that the authentication was successful. """  # noqa

    def __init__(self):
        self.identifier = PostgresBackendMessageIdentifier.AUTHENTICATION_REQUEST
        self.backend_capable = True
        self.frontend_capable = False
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .add_int32(0) \
            .write(write_file=write_file)


class AuthenticationClearTextPassword(PostgresMessage):
    """
    AuthenticationCleartextPassword (B)
    Byte1('R')
    Identifies the message as an authentication request.

    Int32(8)
    Length of message contents in bytes, including self.

    Int32(3)
    Specifies that a clear-text password is required. """  # noqa

    def __init__(self):
        self.identifier = PostgresBackendMessageIdentifier.AUTHENTICATION_REQUEST
        self.backend_capable = True
        self.frontend_capable = False
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .add_int32(3) \
            .write(write_file=write_file)


class ReadyForQuery(PostgresMessage):
    """
    ReadyForQuery (B)
    Byte1('Z')
    Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.

    Int32(5)
    Length of message contents in bytes, including self.

    Byte1
     Current backend transaction status indicator. Possible values are 'I' if idle (not in a transaction block);
    'T' if in a transaction block; or 'E' if in a failed transaction block (queries will be rejected until block is
    ended)."""  # noqa

    transaction_status: bytes

    def __init__(self, transaction_status=None):
        self.identifier = PostgresBackendMessageIdentifier.READY_FOR_QUERY
        self.backend_capable = True
        self.frontend_capable = False
        self.transaction_status = transaction_status or b'I'
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .add_char(self.transaction_status) \
            .write(write_file=write_file)


class CommandComplete(PostgresMessage):
    """
    CommandComplete (B)
    Byte1('C')
    Identifies the message as a command-completed response.

    Int32
    Length of message contents in bytes, including self.

    String
    The command tag. This is usually a single word that identifies which SQL command was completed.

    For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted. oid used to be the object ID of the inserted row if rows was 1 and the target table had OIDs, but OIDs system columns are not supported anymore; therefore oid is always 0.

    For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.

    For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.

    For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.

    For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.

    For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.

    For a COPY command, the tag is COPY rows where rows is the number of rows copied. (Note: the row count appears only in PostgreSQL 8.2 and later.) """  # noqa

    tag: bytes

    def __init__(self, tag: bytes):
        self.identifier = PostgresBackendMessageIdentifier.COMPLETE
        self.tag = tag
        self.backend_capable = True
        self.frontend_capable = False
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .add_string(self.tag) \
            .write(write_file=write_file)


class BindComplete(PostgresMessage):
    """
    BindComplete (B)
    Byte1('2')
    Identifies the message as a Bind-complete indicator.

    Int32(4)
    Length of message contents in bytes, including self. """

    def __init__(self):
        self.identifier = PostgresBackendMessageIdentifier.BIND_COMPLETE
        self.backend_capable = True
        self.frontend_capable = False
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .write(write_file=write_file)


class Error(PostgresMessage):
    """
    ErrorResponse (B)
    Byte1('E')
    Identifies the message as an error.

    Int32
    Length of message contents in bytes, including self.

    The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can
    appear in any order. For each field there is the following:

    Byte1 A code identifying the field type; if zero, this is the message terminator and no string follows. The
    presently defined field types are listed in Section 55.8. Since more field types might be added in future,
    frontends should silently ignore fields of unrecognized type.

    String
    The field value. """  # noqa
    severity: bytes
    code: bytes
    message: bytes

    def __init__(self, severity: bytes, code: bytes, message: bytes):
        self.identifier = PostgresBackendMessageIdentifier.ERROR
        self.backend_capable = True
        self.frontend_capable = False
        self.severity = severity
        self.code = code
        self.message = message
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .add_char(b'S') \
            .add_string(self.severity) \
            .add_char(b'C') \
            .add_string(self.code) \
            .add_char(b'M') \
            .add_string(self.message) \
            .add_char(b'\x00') \
            .write(write_file=write_file)

    @staticmethod
    def from_answer(error_code: bytes, error_message: bytes):
        return Error(severity=b"ERROR", code=error_code, message=error_message)


class ConnectionFailure(Error):
    def __init__(self, message: str = None, charset: str = "UTF-8"):
        if message is None:
            message = "Connection Failure occurred."
        super().__init__(severity="FATAL".encode(encoding=charset), code="08006".encode(encoding=charset),
                         message=message.encode(encoding=charset))


class InvalidSQLStatementName(Error):
    def __init__(self, message: str = None, charset: str = "UTF-8"):
        if message is None:
            message = "Invalid SQL Statement Name"
            super().__init__(severity="FATAL".encode(encoding=charset), code="26000".encode(encoding=charset),
                             message=message.encode(encoding=charset))


class DataException(Error):
    def __init__(self, message: str = None, charset: str = "UTF-8", code: str = "22000"):
        if message is None:
            message = "Data Exception"
            super().__init__(severity="FATAL".encode(encoding=charset), code=code.encode(encoding=charset),
                             message=message.encode(encoding=charset))


class ParameterStatus(PostgresMessage):
    """
    ParameterStatus (B)
    Byte1('S')
    Identifies the message as a run-time parameter status report.

    Int32
    Length of message contents in bytes, including self.

    String
    The name of the run-time parameter being reported.

    String
    The current value of the parameter. """

    def __init__(self, name: bytes, value: bytes):
        self.identifier = PostgresBackendMessageIdentifier.PARAMETER
        self.backend_capable = True
        self.frontend_capable = False
        self.name = name
        self.value = value
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .add_string(self.name) \
            .add_string(self.value) \
            .write(write_file=write_file)


class RowDescriptions(PostgresMessage):
    """
    RowDescription (B)
    Byte1('T')
    Identifies the message as a row description.

    Int32
    Length of message contents in bytes, including self.

    Int16
    Specifies the number of fields in a row (can be zero).

    Then, for each field, there is the following:

    String
    The field name.

    Int32
    If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.

    Int16
    If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.

    Int32
    The object ID of the field's data type.

    Int16
    The data type size (see pg_type.typlen). Note that negative values denote variable-width types.

    Int32
    The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.

    Int16
    The format code being used for the field. Currently will be zero (text) or one (binary). In a
    RowDescription returned from the statement variant of Describe, the format code is not yet known and will always
    be zero."""  # noqa

    fields: Sequence[PostgresField]

    def __init__(self, fields: Sequence[PostgresField]):
        self.identifier = PostgresBackendMessageIdentifier.ROW_DESCRIPTION
        self.backend_capable = True
        self.frontend_capable = False
        self.fields = fields
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .add_int16(len(self.fields)) \
            .add_fields(self.fields) \
            .write(write_file=write_file)


class ParameterDescription(PostgresMessage):
    """
    ParameterDescription (B)
    Byte1('t')
    Identifies the message as a parameter description.

    Int32
    Length of message contents in bytes, including self.

    Int16
    The number of parameters used by the statement (can be zero).

    Then, for each parameter, there is the following:

    Int32
    Specifies the object ID of the parameter data type. """

    def __init__(self, parameters: Sequence):
        self.identifier = PostgresBackendMessageIdentifier.PARAMETER_DESCRIPTION
        self.backend_capable = True
        self.frontend_capable = False
        self.num_params = len(parameters)
        self.parameters = parameters
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        packet = self.get_packet_builder().add_int16(self.num_params)
        for param in self.parameters:
            packet = packet.add_int32(param)
        packet.write(write_file=write_file)


class DataRow(PostgresMessage):
    """
    DataRow (B)
    Byte1('D')
    Identifies the message as a data row.

    Int32
    Length of message contents in bytes, including self.

    Int16
    The number of column values that follow (possibly zero).

    Next, the following pair of fields appear for each column:

    Int32
     The length of the column value, in bytes (this count does not include itself). Can be zero. As a special
    case, -1 indicates a NULL column value. No value bytes follow in the NULL case.

    Byten
    The value of the column, in the format indicated by the associated format code. n is the above length. """  # noqa

    rows: Sequence[Sequence[bytes]]
    num_cols: int

    def __init__(self, rows: Sequence[Sequence[bytes]]):
        self.identifier = PostgresBackendMessageIdentifier.DATA_ROW
        self.backend_capable = True
        self.frontend_capable = False
        if len(rows) != 0:
            self.num_cols = len(rows[0])
        else:
            self.num_cols = 0
        self.rows = rows
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        for row in self.rows:
            self.get_packet_builder() \
                .add_int16(self.num_cols) \
                .add_row(row) \
                .write(write_file=write_file)


class NegotiateProtocolVersion(PostgresMessage):
    """
    NegotiateProtocolVersion (B)
    Byte1('v')
    Identifies the message as a protocol version negotiation message.

    Int32
    Length of message contents in bytes, including self.

    Int32
    Newest minor protocol version supported by the server for the major protocol version requested by the client.

    Int32
    Number of protocol options not recognized by the server.

    Then, for protocol option not recognized by the server, there is the following:

    String
    The option name. """

    def __init__(self, major_version, minor_version, option_not_recognized=None):
        self.identifier = PostgresBackendMessageIdentifier.NEGOTIATE_VERSION
        self.backend_capable = True
        self.frontend_capable = False
        self.major_version = major_version
        self.minor_version = minor_version
        self.option_not_recognized = option_not_recognized
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        packet_builder = self.get_packet_builder() \
            .add_int32(self.major_version) \
            .add_int32(self.minor_version)
        if self.option_not_recognized:
            packet_builder = packet_builder.add_string(self.option_not_recognized)

        packet_builder.write(write_file=write_file)


class ParseComplete(PostgresMessage):
    """
    ParseComplete (B)
    Byte1('1')
    Identifies the message as a Parse-complete indicator.

    Int32(4)
    Length of message contents in bytes, including self. """

    def __init__(self):
        self.identifier = PostgresBackendMessageIdentifier.PARSE_COMPLETE
        self.backend_capable = True
        self.frontend_capable = False
        super().__init__()

    def send_internal(self, write_file: BinaryIO):
        self.get_packet_builder() \
            .write(write_file=write_file)


class Query(PostgresMessage):
    """
    Query (F)
    Byte1('Q')
    Identifies the message as a simple query.

    Int32
    Length of message contents in bytes, including self.

    String
    The query string itself. """

    sql: bytes
    length: int

    def __init__(self):
        self.sql = b''
        self.length = 0
        self.identifier = PostgresFrontendMessageIdentifier.QUERY
        self.backend_capable = False
        self.frontend_capable = True
        super().__init__()

    def read(self, packet_reader: PostgresPacketReader):
        self.length = packet_reader.read_int32()
        self.sql = packet_reader.read_bytes(self.length - 4)
        return self

    def get_parsed_sql(self, encoding=None):
        if not encoding:
            encoding = 'utf-8'
        try:
            sql = self.sql.decode('utf-8')
        except Exception:
            raise Exception(f'SQL contains non {encoding} values: {self.sql}')
        # Remove null bytes from end of sql statement. This is important.
        sql = strip_null_byte(sql)
        sql = clear_sql(sql)
        return sql


class Terminate(PostgresMessage):
    """
    Terminate (F)
    Byte1('X')
    Identifies the message as a termination.

    Int32(4)
    Length of message contents in bytes, including self. """

    def __init__(self):
        self.identifier = PostgresFrontendMessageIdentifier.TERMINATE
        self.backend_capable = False
        self.frontend_capable = True
        super().__init__()

    def read(self, packet_reader: PostgresPacketReader):
        return self


class BaseFrontendMessage(PostgresMessage):
    def __init__(self):
        self.backend_capable = False
        self.frontend_capable = True
        super().__init__()

    def read(self, packet_reader: PostgresPacketReader):
        self.length = packet_reader.read_int32()
        if (self.length - 4) > 0:
            self.response = packet_reader.read_bytes(self.length - 4)
        return self


class Parse(BaseFrontendMessage):
    """
    Parse (F)
    Byte1('P')
    Identifies the message as a Parse command.

    Int32
    Length of message contents in bytes, including self.

    String
    The name of the destination prepared statement (an empty string selects the unnamed prepared statement).

    String
    The query string to be parsed.

    Int16 The number of parameter data types specified (can be zero). Note that this is not an indication of the
    number of parameters that might appear in the query string, only the number that the frontend wants to pre-specify
    types for.

    Then, for each parameter, there is the following:

    Int32 Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type
    unspecified."""

    def __init__(self):
        self.identifier = PostgresFrontendMessageIdentifier.PARSE
        self.name = None
        self.query = None
        self.num_params = None
        self.parameters = []
        super().__init__()

    def read(self, packet_reader: PostgresPacketReader):
        self.length = packet_reader.read_int32()
        self.name = packet_reader.read_string()
        self.query = packet_reader.read_string()
        self.num_params = packet_reader.read_int16()
        for i in range(self.num_params):
            self.parameters.append(packet_reader.read_int32())
        return self


class Bind(BaseFrontendMessage):
    """
    Bind (F)
    Byte1('B')
    Identifies the message as a Bind command.

    Int32
    Length of message contents in bytes, including self.

    String
    The name of the destination portal (an empty string selects the unnamed portal).

    String
    The name of the source prepared statement (an empty string selects the unnamed prepared statement).

    Int16 The number of parameter format codes that follow (denoted C below). This can be zero to indicate that there
    are no parameters or that the parameters all use the default format (text); or one, in which case the specified
    format code is applied to all parameters; or it can equal the actual number of parameters.

    Int16[C]
    The parameter format codes. Each must presently be zero (text) or one (binary).

    Int16 The number of parameter values that follow (possibly zero). This must match the number of parameters needed
    by the query.

    Next, the following pair of fields appear for each parameter:

    Int32 The length of the parameter value, in bytes (this count does not include itself). Can be zero. As a special
    case, -1 indicates a NULL parameter value. No value bytes follow in the NULL case.

    Byten
    The value of the parameter, in the format indicated by the associated format code. n is the above length.

    After the last parameter, the following fields appear:

    Int16 The number of result-column format codes that follow (denoted R below). This can be zero to indicate that
    there are no result columns or that the result columns should all use the default format (text); or one,
    in which case the specified format code is applied to all result columns (if any); or it can equal the actual
    number of result columns of the query.

    Int16[R]
    The result-column format codes. Each must presently be zero (text) or one (binary). """

    def __init__(self):
        self.identifier = PostgresFrontendMessageIdentifier.BIND
        self.length = None
        self.name = None
        self.statement_name = None
        self.format_codes = []  # 0=text 1=binary
        self.parameters = []
        self.result_format_codes = []

        super().__init__()

    def read(self, packet_reader: PostgresPacketReader):
        self.length = packet_reader.read_int32()
        self.name = packet_reader.read_string()
        self.statement_name = packet_reader.read_string()
        num_format_codes = packet_reader.read_int16()
        for _ in range(num_format_codes):
            self.format_codes.append(packet_reader.read_int16())
        num_parameters = packet_reader.read_int16()
        for _ in range(num_parameters):
            param_length = packet_reader.read_int32()
            if param_length == -1:
                self.parameters.append(None)
            else:
                self.parameters.append(packet_reader.read_bytes(param_length))
        num_result_format_codes = packet_reader.read_int16()
        for _ in range(num_result_format_codes):
            self.result_format_codes.append(packet_reader.read_int16())
        return self


class Execute(BaseFrontendMessage):
    """
    Execute (F)
    Byte1('E')
    Identifies the message as an Execute command.

    Int32
    Length of message contents in bytes, including self.

    String
    The name of the portal to execute (an empty string selects the unnamed portal).

    Int32 Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero
    denotes “no limit”."""

    def __init__(self):
        self.identifier = PostgresFrontendMessageIdentifier.EXECUTE
        self.length = None
        self.name = None
        self.max_rows_ret = None
        super().__init__()

    def read(self, packet_reader: PostgresPacketReader):
        self.length = packet_reader.read_int32()
        self.name = packet_reader.read_string()
        self.max_rows_ret = packet_reader.read_int32()
        return self


class Sync(BaseFrontendMessage):
    """
    Sync (F)
    Byte1('S')
    Identifies the message as a Sync command.

    Int32(4)
    Length of message contents in bytes, including self. """

    def __init__(self):
        self.identifier = PostgresFrontendMessageIdentifier.SYNC
        super().__init__()


class Describe(BaseFrontendMessage):
    """
    Describe (F)
    Byte1('D')
    Identifies the message as a Describe command.

    Int32
    Length of message contents in bytes, including self.

    Byte1
    'S' to describe a prepared statement; or 'P' to describe a portal.

    String The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared
    statement or portal)."""

    def __init__(self):
        self.identifier = PostgresFrontendMessageIdentifier.DESCRIBE
        self.length = None
        self.describe_type = None
        self.name = None
        super().__init__()

    def read(self, packet_reader: PostgresPacketReader):
        self.length = packet_reader.read_int32()
        self.describe_type = packet_reader.read_byte()
        self.name = packet_reader.read_string()
        return self


IMPLEMENTED_BACKEND_POSTGRES_MESSAGE_CLASSES = [
    NoticeResponse, AuthenticationOk, AuthenticationClearTextPassword, ReadyForQuery, CommandComplete, Error,
    RowDescriptions, DataRow, NegotiateProtocolVersion, ParameterStatus, ParseComplete, BindComplete,
    ParameterDescription
]
IMPLEMENTED_FRONTEND_POSTGRES_MESSAGE_CLASSES = [
    Query, Terminate, Parse, Bind, Execute, Sync, Describe
]
FE_MESSAGE_MAP: Dict[PostgresFrontendMessageIdentifier, Type[PostgresMessage]] = {
    PostgresFrontendMessageIdentifier.QUERY: Query,
    PostgresFrontendMessageIdentifier.TERMINATE: Terminate,
    PostgresFrontendMessageIdentifier.PARSE: Parse,
    PostgresFrontendMessageIdentifier.BIND: Bind,
    PostgresFrontendMessageIdentifier.EXECUTE: Execute,
    PostgresFrontendMessageIdentifier.SYNC: Sync,
    PostgresFrontendMessageIdentifier.DESCRIBE: Describe
}
SUPPORTED_AUTH_TYPES = [PostgresAuthType.PASSWORD]

# Below Lies Unimplemented Messages

"""
'''
AuthenticationKerberosV5 (B)
Byte1('R')
Identifies the message as an authentication request.

Int32(8)
Length of message contents in bytes, including self.

Int32(2)
Specifies that Kerberos V5 authentication is required. '''

'''
AuthenticationMD5Password (B)
Byte1('R')
Identifies the message as an authentication request.

Int32(12)
Length of message contents in bytes, including self.

Int32(5)
Specifies that an MD5-encrypted password is required.

Byte4
The salt to use when encrypting the password. '''

'''
AuthenticationSCMCredential (B)
Byte1('R')
Identifies the message as an authentication request.

Int32(8)
Length of message contents in bytes, including self.

Int32(6)
Specifies that an SCM credentials message is required. '''

'''
AuthenticationGSS (B)
Byte1('R')
Identifies the message as an authentication request.

Int32(8)
Length of message contents in bytes, including self.

Int32(7)
Specifies that GSSAPI authentication is required. '''

'''
AuthenticationGSSContinue (B)
Byte1('R')
Identifies the message as an authentication request.

Int32
Length of message contents in bytes, including self.

Int32(8)
Specifies that this message contains GSSAPI or SSPI data.

Byten
GSSAPI or SSPI authentication data. '''

'''
AuthenticationSSPI (B)
Byte1('R')
Identifies the message as an authentication request.

Int32(8)
Length of message contents in bytes, including self.

Int32(9)
Specifies that SSPI authentication is required. '''

'''
AuthenticationSASL (B)
Byte1('R')
Identifies the message as an authentication request.

Int32
Length of message contents in bytes, including self.

Int32(10)
Specifies that SASL authentication is required.

The message body is a list of SASL authentication mechanisms, in the server's order of preference. A zero byte is
required as terminator after the last authentication mechanism name. For each mechanism, there is the following:

String
Name of a SASL authentication mechanism. '''

'''
AuthenticationSASLContinue (B)
Byte1('R')
Identifies the message as an authentication request.

Int32
Length of message contents in bytes, including self.

Int32(11)
Specifies that this message contains a SASL challenge.

Byten
SASL data, specific to the SASL mechanism being used. '''

'''
AuthenticationSASLFinal (B)
Byte1('R')
Identifies the message as an authentication request.

Int32
Length of message contents in bytes, including self.

Int32(12)
Specifies that SASL authentication has completed.

Byten
SASL outcome "additional data", specific to the SASL mechanism being used. '''

'''BackendKeyData (B) Byte1('K') Identifies the message as cancellation key data. The frontend must save these values
if it wishes to be able to issue CancelRequest messages later.

Int32(12)
Length of message contents in bytes, including self.

Int32
The process ID of this backend.

Int32
The secret key of this backend. '''

'''
CancelRequest (F)
Int32(16)
Length of message contents in bytes, including self.

Int32(80877102) The cancel request code. The value is chosen to contain 1234 in the most significant 16 bits,
and 5678 in the least significant 16 bits. (To avoid confusion, this code must not be the same as any protocol
version number.)

Int32
The process ID of the target backend.

Int32
The secret key for the target backend. '''

'''
Close (F)
Byte1('C')
Identifies the message as a Close command.

Int32
Length of message contents in bytes, including self.

Byte1
'S' to close a prepared statement; or 'P' to close a portal.

String The name of the prepared statement or portal to close (an empty string selects the unnamed prepared statement
or portal).'''

'''
CloseComplete (B)
Byte1('3')
Identifies the message as a Close-complete indicator.

Int32(4)
Length of message contents in bytes, including self. '''

'''
CommandComplete (B)
Byte1('C')
Identifies the message as a command-completed response.

Int32
Length of message contents in bytes, including self.

String
The command tag. This is usually a single word that identifies which SQL command was completed.

For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted. oid used to be the
object ID of the inserted row if rows was 1 and the target table had OIDs, but OIDs system columns are not supported
anymore; therefore oid is always 0.

For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.

For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.

For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.

For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.

For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.

For a COPY command, the tag is COPY rows where rows is the number of rows copied. (Note: the row count appears only
in PostgreSQL 8.2 and later.)'''

'''
CopyData (F & B)
Byte1('d')
Identifies the message as COPY data.

Int32
Length of message contents in bytes, including self.

Byten Data that forms part of a COPY data stream. Messages sent from the backend will always correspond to single
data rows, but messages sent by frontends might divide the data stream arbitrarily.'''

'''
CopyDone (F & B)
Byte1('c')
Identifies the message as a COPY-complete indicator.

Int32(4)
Length of message contents in bytes, including self. '''

'''
CopyFail (F)
Byte1('f')
Identifies the message as a COPY-failure indicator.

Int32
Length of message contents in bytes, including self.

String
An error message to report as the cause of failure. '''

'''CopyInResponse (B) Byte1('G') Identifies the message as a Start Copy In response. The frontend must now send
copy-in data (if not prepared to do so, send a CopyFail message).

Int32
Length of message contents in bytes, including self.

Int8 0 indicates the overall COPY format is textual (rows separated by newlines, columns separated by separator
characters, etc.). 1 indicates the overall copy format is binary (similar to DataRow format). See COPY for more
information.

Int16
The number of columns in the data to be copied (denoted N below).

Int16[N] The format codes to be used for each column. Each must presently be zero (text) or one (binary). All must be
zero if the overall copy format is textual.'''

'''
CopyOutResponse (B)
Byte1('H')
Identifies the message as a Start Copy Out response. This message will be followed by copy-out data.

Int32
Length of message contents in bytes, including self.

Int8 0 indicates the overall COPY format is textual (rows separated by newlines, columns separated by separator
characters, etc.). 1 indicates the overall copy format is binary (similar to DataRow format). See COPY for more
information.

Int16
The number of columns in the data to be copied (denoted N below).

Int16[N] The format codes to be used for each column. Each must presently be zero (text) or one (binary). All must be
zero if the overall copy format is textual.'''

'''
CopyBothResponse (B)
Byte1('W')
Identifies the message as a Start Copy Both response. This message is used only for Streaming Replication.

Int32
Length of message contents in bytes, including self.

Int8 0 indicates the overall COPY format is textual (rows separated by newlines, columns separated by separator
characters, etc.). 1 indicates the overall copy format is binary (similar to DataRow format). See COPY for more
information.

Int16
The number of columns in the data to be copied (denoted N below).

Int16[N] The format codes to be used for each column. Each must presently be zero (text) or one (binary). All must be
zero if the overall copy format is textual.'''

'''
Describe (F)
Byte1('D')
Identifies the message as a Describe command.

Int32
Length of message contents in bytes, including self.

Byte1
'S' to describe a prepared statement; or 'P' to describe a portal.

String The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared
statement or portal).'''

'''
EmptyQueryResponse (B)
Byte1('I')
Identifies the message as a response to an empty query string. (This substitutes for CommandComplete.)

Int32(4)
Length of message contents in bytes, including self. '''

'''
Execute (F)
Byte1('E')
Identifies the message as an Execute command.

Int32
Length of message contents in bytes, including self.

String
The name of the portal to execute (an empty string selects the unnamed portal).

Int32 Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero
denotes “no limit”.'''

'''
Flush (F)
Byte1('H')
Identifies the message as a Flush command.

Int32(4)
Length of message contents in bytes, including self. '''

'''
FunctionCall (F)
Byte1('F')
Identifies the message as a function call.

Int32
Length of message contents in bytes, including self.

Int32
Specifies the object ID of the function to call.

Int16 The number of argument format codes that follow (denoted C below). This can be zero to indicate that there are
no arguments or that the arguments all use the default format (text); or one, in which case the specified format code
is applied to all arguments; or it can equal the actual number of arguments.

Int16[C]
The argument format codes. Each must presently be zero (text) or one (binary).

Int16
Specifies the number of arguments being supplied to the function.

Next, the following pair of fields appear for each argument:

Int32 The length of the argument value, in bytes (this count does not include itself). Can be zero. As a special
case, -1 indicates a NULL argument value. No value bytes follow in the NULL case.

Byten
The value of the argument, in the format indicated by the associated format code. n is the above length.

After the last argument, the following field appears:

Int16
The format code for the function result. Must presently be zero (text) or one (binary). '''

'''
FunctionCallResponse (B)
Byte1('V')
Identifies the message as a function call result.

Int32
Length of message contents in bytes, including self.

Int32 The length of the function result value, in bytes (this count does not include itself). Can be zero. As a
special case, -1 indicates a NULL function result. No value bytes follow in the NULL case.

Byten
The value of the function result, in the format indicated by the associated format code. n is the above length. '''

'''
GSSENCRequest (F)
Int32(8)
Length of message contents in bytes, including self.

Int32(80877104) The GSSAPI Encryption request code. The value is chosen to contain 1234 in the most significant 16
bits, and 5680 in the least significant 16 bits. (To avoid confusion, this code must not be the same as any protocol
version number.)'''

'''GSSResponse (F) Byte1('p') Identifies the message as a GSSAPI or SSPI response. Note that this is also used for
SASL and password response messages. The exact message type can be deduced from the context.

Int32
Length of message contents in bytes, including self.

Byten
GSSAPI/SSPI specific message data. '''

'''
NoData (B)
Byte1('n')
Identifies the message as a no-data indicator.

Int32(4)
Length of message contents in bytes, including self. '''

'''
NotificationResponse (B)
Byte1('A')
Identifies the message as a notification response.

Int32
Length of message contents in bytes, including self.

Int32
The process ID of the notifying backend process.

String
The name of the channel that the notify has been raised on.

String
The “payload” string passed from the notifying process. '''

'''
Parse (F)
Byte1('P')
Identifies the message as a Parse command.

Int32
Length of message contents in bytes, including self.

String
The name of the destination prepared statement (an empty string selects the unnamed prepared statement).

String
The query string to be parsed.

Int16 The number of parameter data types specified (can be zero). Note that this is not an indication of the number
of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.

Then, for each parameter, there is the following:

Int32 Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type
unspecified.'''

'''PasswordMessage (F) Byte1('p') Identifies the message as a password response. Note that this is also used for
GSSAPI, SSPI and SASL response messages. The exact message type can be deduced from the context.

Int32
Length of message contents in bytes, including self.

String
The password (encrypted, if requested). '''

'''PortalSuspended (B) Byte1('s') Identifies the message as a portal-suspended indicator. Note this only appears if
an Execute message's row-count limit was reached.

Int32(4)
Length of message contents in bytes, including self. '''

'''SASLInitialResponse (F) Byte1('p') Identifies the message as an initial SASL response. Note that this is also used
for GSSAPI, SSPI and password response messages. The exact message type is deduced from the context.

Int32
Length of message contents in bytes, including self.

String
Name of the SASL authentication mechanism that the client selected.

Int32
Length of SASL mechanism specific "Initial Client Response" that follows, or -1 if there is no Initial Response.

Byten
SASL mechanism specific "Initial Response". '''

'''SASLResponse (F) Byte1('p') Identifies the message as a SASL response. Note that this is also used for GSSAPI,
SSPI and password response messages. The exact message type can be deduced from the context.

Int32
Length of message contents in bytes, including self.

Byten
SASL mechanism specific message data. '''

'''
StartupMessage (F)
Int32
Length of message contents in bytes, including self.

Int32(196608) The protocol version number. The most significant 16 bits are the major version number (3 for the
protocol described here). The least significant 16 bits are the minor version number (0 for the protocol described
here).

The protocol version number is followed by one or more pairs of parameter name and value strings. A zero byte is
required as a terminator after the last name/value pair. Parameters can appear in any order. user is required,
others are optional. Each parameter is specified as:

String
The parameter name. Currently recognized names are:

user
The database user name to connect as. Required; there is no default.

database
The database to connect to. Defaults to the user name.

options Command-line arguments for the backend. (This is deprecated in favor of setting individual run-time
parameters.) Spaces within this string are considered to separate arguments, unless escaped with a backslash
write to represent a literal backslash.

replication Used to connect in streaming replication mode, where a small set of replication commands can be issued
instead of SQL statements. Value can be true, false, or database, and the default is false. See Section 55.4 for
details.

In addition to the above, other parameters may be listed. Parameter names beginning with _pq_. are reserved for use
as protocol extensions, while others are treated as run-time parameters to be set at backend start time. Such
settings will be applied during backend start (after parsing the command-line arguments if any) and will act as
session defaults.

String
The parameter value. '''
"""
