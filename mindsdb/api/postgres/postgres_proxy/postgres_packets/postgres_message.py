from typing import Union, BinaryIO

from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_message_identifiers import \
    PostgresBackendMessageIdentifier, PostgresFrontendMessageIdentifier
from mindsdb.api.postgres.postgres_proxy.postgres_packets.postgres_packets import PostgresPacketReader, \
    PostgresPacketBuilder


class PostgresMessage:
    identifier: Union[PostgresBackendMessageIdentifier, PostgresFrontendMessageIdentifier]
    backend_capable: bool
    frontend_capable: bool

    def __init__(self):
        pass

    def send(self, write_file: BinaryIO):
        pass

    def read(self, packet_reader: PostgresPacketReader):
        pass

    def get_packet_builder(self) -> PostgresPacketBuilder:
        ppb = PostgresPacketBuilder()
        ppb.set_identifier(self.identifier)
        return ppb
