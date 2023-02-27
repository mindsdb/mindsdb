import struct
from typing import List, Any, BinaryIO, Sequence, Union
from mindsdb.utilities.log import get_log
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
        self.logger = get_log('postgres_packet_reader')

    def read_byte(self):
        return self.read_bytes(1)

    def read_bytes(self, n):
        data = self.buffer.read(n)
        if not data:
            raise PostgresEmptyDataException("Expected data inside of buffer when performing read_bytes")
        else:
            print(data)
            return data

    def read_bytes_timeout(self, n, timeout=60):
        cur = time.time()
        end = cur + timeout
        #This is a reason to switch from socketserver to asyncio
        while cur < end:
            cur = time.time()
            data = self.buffer.read(n)
            if not data:
                pass
            else:
                print(data)
                return data
        raise PostgresEmptyDataException("Expected data inside of buffer when performing read_bytes")

    def read_int32(self):
        data = self.read_bytes(4)
        return struct.unpack("!i", data)[0]

    def read_parameters(self, n):
        data = self.read_bytes(n)
        return data.split(b'\x00')

    def read_verify_ssl_request(self):
        length = self.read_int32()
        code = self.read_int32()
        if length != 8 and code != 80877103:
            raise UnsupportedSSLRequest("Code %s of len %s" % (code, length))

    def read_startup_message(self):
        length = self.read_int32()
        version = self.read_int32()
        major_version = version >> 16
        minor_version = version & 0xffff
        message = self.read_parameters(length - 8)
        self.logger.info('PSQL Startup Message %d.%d : %s' % (major_version, minor_version, message))

    def read_authentication(self):
        auth_type = self.read_byte()
        try:
            auth_type = PostgresAuthType(auth_type)
        except Exception as e:
            raise UnsupportedPostgresAuthException(e)
        if auth_type not in self.supported_auth_types:
            raise UnsupportedPostgresAuthException("%s is not a supported auth type identifier" % auth_type)
        length = self.read_int32()
        _ = self.read_bytes(length - 4)  # password. Do something with later. We read to clear buffer.

    def read_message(self):
        message_type = self.read_byte()
        print("Message type", message_type)
        try:
            message_type = PostgresFrontendMessageIdentifier(message_type)
        except Exception as _:
            raise UnsupportedPostgresMessageType("%s is not a supported frontend message identifier" % message_type)

        if message_type in self.fe_message_map:
            return self.fe_message_map[message_type]().read(self)
        else:
            raise UnsupportedPostgresMessageType("%s is not a supported frontend message identifier" % message_type.value)


class PostgresPacketBuilder:
    identifier: bytes
    pack_string: str
    length: int
    pack_args: List[Any]

    def __init__(self):
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

    def write(self, write_file: BinaryIO):
        if len(self.identifier) == 0:
            raise Exception("Can't write without identifier.")
        pack = "!c"
        if self.length > 0:
            # If we have a length, add it to the pack string struct will take, as well as to the top of pack_args.
            # Ensures 'i' in pack and self.length in 2nd position of respective variables.
            # Also adds 4 to length to include int32 rep of length
            self.length += 4
            pack += "i"
            self.pack_args = [self.length] + self.pack_args
        pack += self.pack_string
        print("pack string", self.pack_string)
        print("identifier", self.identifier)
        print("pack args", self.pack_args)
        l = struct.pack(pack, self.identifier, *self.pack_args)
        write_file.write(l)

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
