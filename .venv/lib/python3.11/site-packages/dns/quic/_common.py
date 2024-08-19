# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import copy
import functools
import socket
import struct
import time
from typing import Any, Optional

import aioquic.quic.configuration  # type: ignore
import aioquic.quic.connection  # type: ignore

import dns.inet

QUIC_MAX_DATAGRAM = 2048
MAX_SESSION_TICKETS = 8
# If we hit the max sessions limit we will delete this many of the oldest connections.
# The value must be a integer > 0 and <= MAX_SESSION_TICKETS.
SESSIONS_TO_DELETE = MAX_SESSION_TICKETS // 4


class UnexpectedEOF(Exception):
    pass


class Buffer:
    def __init__(self):
        self._buffer = b""
        self._seen_end = False

    def put(self, data, is_end):
        if self._seen_end:
            return
        self._buffer += data
        if is_end:
            self._seen_end = True

    def have(self, amount):
        if len(self._buffer) >= amount:
            return True
        if self._seen_end:
            raise UnexpectedEOF
        return False

    def seen_end(self):
        return self._seen_end

    def get(self, amount):
        assert self.have(amount)
        data = self._buffer[:amount]
        self._buffer = self._buffer[amount:]
        return data


class BaseQuicStream:
    def __init__(self, connection, stream_id):
        self._connection = connection
        self._stream_id = stream_id
        self._buffer = Buffer()
        self._expecting = 0

    def id(self):
        return self._stream_id

    def _expiration_from_timeout(self, timeout):
        if timeout is not None:
            expiration = time.time() + timeout
        else:
            expiration = None
        return expiration

    def _timeout_from_expiration(self, expiration):
        if expiration is not None:
            timeout = max(expiration - time.time(), 0.0)
        else:
            timeout = None
        return timeout

    # Subclass must implement receive() as sync / async and which returns a message
    # or raises UnexpectedEOF.

    def _encapsulate(self, datagram):
        l = len(datagram)
        return struct.pack("!H", l) + datagram

    def _common_add_input(self, data, is_end):
        self._buffer.put(data, is_end)
        try:
            return self._expecting > 0 and self._buffer.have(self._expecting)
        except UnexpectedEOF:
            return True

    def _close(self):
        self._connection.close_stream(self._stream_id)
        self._buffer.put(b"", True)  # send EOF in case we haven't seen it.


class BaseQuicConnection:
    def __init__(
        self, connection, address, port, source=None, source_port=0, manager=None
    ):
        self._done = False
        self._connection = connection
        self._address = address
        self._port = port
        self._closed = False
        self._manager = manager
        self._streams = {}
        self._af = dns.inet.af_for_address(address)
        self._peer = dns.inet.low_level_address_tuple((address, port))
        if source is None and source_port != 0:
            if self._af == socket.AF_INET:
                source = "0.0.0.0"
            elif self._af == socket.AF_INET6:
                source = "::"
            else:
                raise NotImplementedError
        if source:
            self._source = (source, source_port)
        else:
            self._source = None

    def close_stream(self, stream_id):
        del self._streams[stream_id]

    def _get_timer_values(self, closed_is_special=True):
        now = time.time()
        expiration = self._connection.get_timer()
        if expiration is None:
            expiration = now + 3600  # arbitrary "big" value
        interval = max(expiration - now, 0)
        if self._closed and closed_is_special:
            # lower sleep interval to avoid a race in the closing process
            # which can lead to higher latency closing due to sleeping when
            # we have events.
            interval = min(interval, 0.05)
        return (expiration, interval)

    def _handle_timer(self, expiration):
        now = time.time()
        if expiration <= now:
            self._connection.handle_timer(now)


class AsyncQuicConnection(BaseQuicConnection):
    async def make_stream(self, timeout: Optional[float] = None) -> Any:
        pass


class BaseQuicManager:
    def __init__(self, conf, verify_mode, connection_factory, server_name=None):
        self._connections = {}
        self._connection_factory = connection_factory
        self._session_tickets = {}
        if conf is None:
            verify_path = None
            if isinstance(verify_mode, str):
                verify_path = verify_mode
                verify_mode = True
            conf = aioquic.quic.configuration.QuicConfiguration(
                alpn_protocols=["doq", "doq-i03"],
                verify_mode=verify_mode,
                server_name=server_name,
            )
            if verify_path is not None:
                conf.load_verify_locations(verify_path)
        self._conf = conf

    def _connect(
        self, address, port=853, source=None, source_port=0, want_session_ticket=True
    ):
        connection = self._connections.get((address, port))
        if connection is not None:
            return (connection, False)
        conf = self._conf
        if want_session_ticket:
            try:
                session_ticket = self._session_tickets.pop((address, port))
                # We found a session ticket, so make a configuration that uses it.
                conf = copy.copy(conf)
                conf.session_ticket = session_ticket
            except KeyError:
                # No session ticket.
                pass
            # Whether or not we found a session ticket, we want a handler to save
            # one.
            session_ticket_handler = functools.partial(
                self.save_session_ticket, address, port
            )
        else:
            session_ticket_handler = None
        qconn = aioquic.quic.connection.QuicConnection(
            configuration=conf,
            session_ticket_handler=session_ticket_handler,
        )
        lladdress = dns.inet.low_level_address_tuple((address, port))
        qconn.connect(lladdress, time.time())
        connection = self._connection_factory(
            qconn, address, port, source, source_port, self
        )
        self._connections[(address, port)] = connection
        return (connection, True)

    def closed(self, address, port):
        try:
            del self._connections[(address, port)]
        except KeyError:
            pass

    def save_session_ticket(self, address, port, ticket):
        # We rely on dictionaries keys() being in insertion order here.  We
        # can't just popitem() as that would be LIFO which is the opposite of
        # what we want.
        l = len(self._session_tickets)
        if l >= MAX_SESSION_TICKETS:
            keys_to_delete = list(self._session_tickets.keys())[0:SESSIONS_TO_DELETE]
            for key in keys_to_delete:
                del self._session_tickets[key]
        self._session_tickets[(address, port)] = ticket


class AsyncQuicManager(BaseQuicManager):
    def connect(self, address, port=853, source=None, source_port=0):
        raise NotImplementedError
