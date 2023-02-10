import struct
from typing import List, Any, BinaryIO, Sequence

from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_fields import PostgresField
from mindsdb.api.postgres.postgres_proxy.postgres_packets.psql_message_formats import PostgresBackendMessageIdentifier


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
        write_file.write(struct.pack(pack, self.identifier, *self.pack_args))

    def add_char(self, s: bytes):
        self.pack_string += 'c'
        self.pack_args.append(s[0])
        return self.add_length(1)

    def add_string(self, s: bytes):
        self.pack_string += 's'
        s = s + b'\x00'
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
        self.pack_string += 's'
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

    def add_rows(self, rows: Sequence[Sequence[bytes]]):
        for row in rows:
            self.add_row(row)
        return self
