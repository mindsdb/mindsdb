import struct
from typing import List, Any, BinaryIO, Sequence, Dict

from mindsdb.api.postgres.postgres_proxy.utilities import strip_null_byte
from mindsdb.utilities import log
import time
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import PostgresField


from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_identifiers import \
    PostgresBackendMessageIdentifier, PostgresFrontendMessageIdentifier, PostgresAuthType


class PostgresEmptyDataException(Exception):
    pass


class UnsupportedSSLRequest(Exception):
    pass


class UnsupportedPostgresAuthException(Exception):
    pass


class UnsupportedPostgresMessageType(Exception):
    pass


class PostgresPacketReader:
    def __init__(self, buffer: BinaryIO):
        from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_formats import FE_MESSAGE_MAP, \
            SUPPORTED_AUTH_TYPES
        self.fe_message_map = FE_MESSAGE_MAP
        self.supported_auth_types = SUPPORTED_AUTH_TYPES
        self.buffer = buffer
        self.logger = log.getLogger(__name__)

    def read_byte(self):
        return self.read_bytes(1)

    def read_bytes(self, n):
        data = self.buffer.read(n)
        self.logger.debug("received data:")
        self.logger.debug(data.hex())
        if not data:
            raise PostgresEmptyDataException("Expected data inside of buffer when performing read_bytes")
        else:
            return data

    def read_string(self):
        result = b""
        while True:
            b = self.read_byte()
            if b == b"\x00":
                break
            result = result + b
        return result

    def read_bytes_timeout(self, n, timeout=60):
        cur = time.time()
        end = cur + timeout
        # This is a reason to switch from socketserver to asyncio
        while cur < end:
            cur = time.time()
            data = self.buffer.read(n)
            if not data:
                pass
            else:
                return data
        raise PostgresEmptyDataException("Expected data inside of buffer when performing read_bytes")

    def read_int16(self):
        data = self.read_bytes(2)
        return struct.unpack("!h", data)[0]

    def read_int32(self):
        data = self.read_bytes(4)
        return struct.unpack("!i", data)[0]

    def read_parameters(self, n):
        data = self.read_bytes(n)
        return data.split(b'\x00')

    def read_verify_ssl_request(self):
        self.logger.debug("reading ssl")
        length = self.read_int32()
        code = self.read_int32()
        if length != 8 and code != 80877103:
            raise UnsupportedSSLRequest("Code %s of len %s" % (code, length))

    def read_startup_message(self) -> Dict[bytes, bytes]:
        self.logger.debug("reading startup message")
        length = self.read_int32()
        version = self.read_int32()
        major_version = version >> 16
        minor_version = version & 0xffff
        message = self.read_parameters(length - 8)
        self.logger.debug('PSQL Startup Message %d.%d : %s' % (major_version, minor_version, message))
        parameters = {}
        while len(message) != 0:
            key = message.pop(0)
            value = message.pop(0)
            parameters[key] = value
        parameters[b"major_version"] = major_version
        parameters[b"minor_version"] = minor_version
        return parameters

    def read_authentication(self, encoding=None):
        try:
            auth_type = self.read_byte()
        except PostgresEmptyDataException:
            # No authentication parameters specified. Which is fine if we're local on a mindsdbuser
            return ''
        try:
            auth_type = PostgresAuthType(auth_type)
        except Exception as e:
            raise UnsupportedPostgresAuthException(e)
        if auth_type not in self.supported_auth_types:
            raise UnsupportedPostgresAuthException("%s is not a supported auth type identifier" % auth_type)
        length = self.read_int32()
        password = strip_null_byte(self.read_bytes(length - 4), encoding=encoding)  # password. Do something with later. We read to clear buffer.
        return password

    def read_message(self):
        try:
            message_type = self.read_byte()
        except PostgresEmptyDataException:
            self.logger.warn("Postgres Proxy: Received empty data string")
            return None
        try:
            message_type = PostgresFrontendMessageIdentifier(message_type)
        except Exception as e:
            raise UnsupportedPostgresMessageType(
                "%s is not a supported frontend message identifier:\n%s" % (message_type, str(e)))

        if message_type in self.fe_message_map:
            self.logger.debug("reading message type %s" % str(message_type.name))
            return self.fe_message_map[message_type]().read(self)
        else:
            raise UnsupportedPostgresMessageType(
                "%s is not a supported frontend message identifier" % message_type.value)


class PostgresPacketBuilder:
    identifier: bytes
    pack_string: str
    length: int
    pack_args: List[Any]

    def __init__(self):
        self.logger = log.getLogger(__name__)
        self.reset()

    def reset(self):
        self.identifier = b''
        self.pack_string = ''
        self.pack_args = []
        self.length = 0

    def set_identifier(self, message_identifier: PostgresBackendMessageIdentifier):
        self.identifier = message_identifier.value
        return self

    def add_length(self, length: int):
        self.length += length
        return self

    def write_char(self, c, write_file: BinaryIO):
        pack = "!c"
        packed_binary = struct.pack(pack, c)
        write_file.write(packed_binary)

    def write(self, write_file: BinaryIO):
        if len(self.identifier) == 0:
            raise Exception("Can't write without identifier.")
        pack = "!c"

        # Send length
        # Ensures 'i' in pack and self.length in 2nd position of respective variables.
        # Also adds 4 to length to include int32 rep of length
        self.length += 4
        pack += "i"
        self.pack_args = [self.length] + self.pack_args

        pack += self.pack_string
        self.logger.debug("writing:")
        self.logger.debug("pack string: %s" % self.pack_string)
        self.logger.debug("identifier: %s" % self.pack_string)
        self.logger.debug("pack args:")
        for arg in self.pack_args:
            self.logger.debug("arg: %s" % str(arg))
        packed_binary = struct.pack(pack, self.identifier, *self.pack_args)
        write_file.write(packed_binary)

    def add_char(self, s: bytes):
        self.pack_string += 'c'
        if len(s) != 1:
            raise Exception("Char must be of length 1 in add_char")
        self.pack_args.append(s)
        return self.add_length(1)

    def add_string(self, s: bytes):
        s = s + b'\x00'
        self.pack_string += str(len(s)) + 's'
        self.pack_args.append(s)
        return self.add_length(len(s))

    def add_int32(self, i):
        self.pack_string += 'i'
        self.pack_args.append(i)
        return self.add_length(4)

    def add_int16(self, h):
        self.pack_string += 'h'
        self.pack_args.append(h)
        return self.add_length(2)

    def add_bytes(self, b: bytes):
        if len(b) == 1:
            self.pack_string += 's'
        else:
            self.pack_string += str(len(b)) + 's'
        self.pack_args.append(b)
        return self.add_length(len(b))

    def add_field(self, field: PostgresField):
        return self.add_string(field.name.encode()) \
            .add_int32(field.table_id) \
            .add_int16(field.column_id) \
            .add_int32(field.object_id) \
            .add_int16(field.dt_size) \
            .add_int32(field.type_modifier) \
            .add_int16(field.format_code)

    def add_fields(self, fields: Sequence[PostgresField]):
        for field in fields:
            self.add_field(field)
        return self

    def add_column_value(self, val: bytes):
        if val == b'NULL':
            self.add_int32(-1)
            return self

        self.add_int32(len(val))
        self.add_bytes(val)
        return self

    def add_row(self, row: Sequence[bytes]):
        for val in row:
            self.add_column_value(val)
        return self
