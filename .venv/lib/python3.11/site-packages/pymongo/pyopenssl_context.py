# Copyright 2019-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""A CPython compatible SSLContext implementation wrapping PyOpenSSL's
context.
"""
from __future__ import annotations

import socket as _socket
import ssl as _stdlibssl
import sys as _sys
import time as _time
from errno import EINTR as _EINTR
from ipaddress import ip_address as _ip_address
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, Union

import cryptography.x509 as x509
import service_identity
from OpenSSL import SSL as _SSL
from OpenSSL import crypto as _crypto

from pymongo.errors import ConfigurationError as _ConfigurationError
from pymongo.errors import _CertificateError  # type:ignore[attr-defined]
from pymongo.ocsp_cache import _OCSPCache
from pymongo.ocsp_support import _load_trusted_ca_certs, _ocsp_callback
from pymongo.socket_checker import SocketChecker as _SocketChecker
from pymongo.socket_checker import _errno_from_exception
from pymongo.write_concern import validate_boolean

if TYPE_CHECKING:
    from ssl import VerifyMode


_T = TypeVar("_T")

try:
    import certifi

    _HAVE_CERTIFI = True
except ImportError:
    _HAVE_CERTIFI = False

PROTOCOL_SSLv23 = _SSL.SSLv23_METHOD
# Always available
OP_NO_SSLv2 = _SSL.OP_NO_SSLv2
OP_NO_SSLv3 = _SSL.OP_NO_SSLv3
OP_NO_COMPRESSION = _SSL.OP_NO_COMPRESSION
# This isn't currently documented for PyOpenSSL
OP_NO_RENEGOTIATION = getattr(_SSL, "OP_NO_RENEGOTIATION", 0)

# Always available
HAS_SNI = True
IS_PYOPENSSL = True

# Base Exception class
SSLError = _SSL.Error

# https://github.com/python/cpython/blob/v3.8.0/Modules/_ssl.c#L2995-L3002
_VERIFY_MAP = {
    _stdlibssl.CERT_NONE: _SSL.VERIFY_NONE,
    _stdlibssl.CERT_OPTIONAL: _SSL.VERIFY_PEER,
    _stdlibssl.CERT_REQUIRED: _SSL.VERIFY_PEER | _SSL.VERIFY_FAIL_IF_NO_PEER_CERT,
}

_REVERSE_VERIFY_MAP = {value: key for key, value in _VERIFY_MAP.items()}


# For SNI support. According to RFC6066, section 3, IPv4 and IPv6 literals are
# not permitted for SNI hostname.
def _is_ip_address(address: Any) -> bool:
    try:
        _ip_address(address)
        return True
    except (ValueError, UnicodeError):
        return False


# According to the docs for socket.send it can raise
# WantX509LookupError and should be retried.
BLOCKING_IO_ERRORS = (_SSL.WantReadError, _SSL.WantWriteError, _SSL.WantX509LookupError)


def _ragged_eof(exc: BaseException) -> bool:
    """Return True if the OpenSSL.SSL.SysCallError is a ragged EOF."""
    return exc.args == (-1, "Unexpected EOF")


# https://github.com/pyca/pyopenssl/issues/168
# https://github.com/pyca/pyopenssl/issues/176
# https://docs.python.org/3/library/ssl.html#notes-on-non-blocking-sockets
class _sslConn(_SSL.Connection):
    def __init__(
        self, ctx: _SSL.Context, sock: Optional[_socket.socket], suppress_ragged_eofs: bool
    ):
        self.socket_checker = _SocketChecker()
        self.suppress_ragged_eofs = suppress_ragged_eofs
        super().__init__(ctx, sock)

    def _call(self, call: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        timeout = self.gettimeout()
        if timeout:
            start = _time.monotonic()
        while True:
            try:
                return call(*args, **kwargs)
            except BLOCKING_IO_ERRORS as exc:
                # Check for closed socket.
                if self.fileno() == -1:
                    if timeout and _time.monotonic() - start > timeout:
                        raise _socket.timeout("timed out") from None
                    raise SSLError("Underlying socket has been closed") from None
                if isinstance(exc, _SSL.WantReadError):
                    want_read = True
                    want_write = False
                elif isinstance(exc, _SSL.WantWriteError):
                    want_read = False
                    want_write = True
                else:
                    want_read = True
                    want_write = True
                self.socket_checker.select(self, want_read, want_write, timeout)
                if timeout and _time.monotonic() - start > timeout:
                    raise _socket.timeout("timed out") from None
                continue

    def do_handshake(self, *args: Any, **kwargs: Any) -> None:
        return self._call(super().do_handshake, *args, **kwargs)

    def recv(self, *args: Any, **kwargs: Any) -> bytes:
        try:
            return self._call(super().recv, *args, **kwargs)
        except _SSL.SysCallError as exc:
            # Suppress ragged EOFs to match the stdlib.
            if self.suppress_ragged_eofs and _ragged_eof(exc):
                return b""
            raise

    def recv_into(self, *args: Any, **kwargs: Any) -> int:
        try:
            return self._call(super().recv_into, *args, **kwargs)
        except _SSL.SysCallError as exc:
            # Suppress ragged EOFs to match the stdlib.
            if self.suppress_ragged_eofs and _ragged_eof(exc):
                return 0
            raise

    def sendall(self, buf: bytes, flags: int = 0) -> None:  # type: ignore[override]
        view = memoryview(buf)
        total_length = len(buf)
        total_sent = 0
        while total_sent < total_length:
            try:
                sent = self._call(super().send, view[total_sent:], flags)
            # XXX: It's not clear if this can actually happen. PyOpenSSL
            # doesn't appear to have any interrupt handling, nor any interrupt
            # errors for OpenSSL connections.
            except OSError as exc:
                if _errno_from_exception(exc) == _EINTR:
                    continue
                raise
            # https://github.com/pyca/pyopenssl/blob/19.1.0/src/OpenSSL/SSL.py#L1756
            # https://www.openssl.org/docs/man1.0.2/man3/SSL_write.html
            if sent <= 0:
                raise OSError("connection closed")
            total_sent += sent


class _CallbackData:
    """Data class which is passed to the OCSP callback."""

    def __init__(self) -> None:
        self.trusted_ca_certs: Optional[list[x509.Certificate]] = None
        self.check_ocsp_endpoint: Optional[bool] = None
        self.ocsp_response_cache = _OCSPCache()


class SSLContext:
    """A CPython compatible SSLContext implementation wrapping PyOpenSSL's
    context.
    """

    __slots__ = ("_protocol", "_ctx", "_callback_data", "_check_hostname")

    def __init__(self, protocol: int):
        self._protocol = protocol
        self._ctx = _SSL.Context(self._protocol)
        self._callback_data = _CallbackData()
        self._check_hostname = True
        # OCSP
        # XXX: Find a better place to do this someday, since this is client
        # side configuration and wrap_socket tries to support both client and
        # server side sockets.
        self._callback_data.check_ocsp_endpoint = True
        self._ctx.set_ocsp_client_callback(callback=_ocsp_callback, data=self._callback_data)

    @property
    def protocol(self) -> int:
        """The protocol version chosen when constructing the context.
        This attribute is read-only.
        """
        return self._protocol

    def __get_verify_mode(self) -> VerifyMode:
        """Whether to try to verify other peers' certificates and how to
        behave if verification fails. This attribute must be one of
        ssl.CERT_NONE, ssl.CERT_OPTIONAL or ssl.CERT_REQUIRED.
        """
        return _REVERSE_VERIFY_MAP[self._ctx.get_verify_mode()]

    def __set_verify_mode(self, value: VerifyMode) -> None:
        """Setter for verify_mode."""

        def _cb(
            _connobj: _SSL.Connection,
            _x509obj: _crypto.X509,
            _errnum: int,
            _errdepth: int,
            retcode: int,
        ) -> bool:
            # It seems we don't need to do anything here. Twisted doesn't,
            # and OpenSSL's SSL_CTX_set_verify let's you pass NULL
            # for the callback option. It's weird that PyOpenSSL requires
            # this.
            # This is optional in pyopenssl >= 20 and can be removed once minimum
            # supported version is bumped
            # See: pyopenssl.org/en/latest/changelog.html#id47
            return bool(retcode)

        self._ctx.set_verify(_VERIFY_MAP[value], _cb)

    verify_mode = property(__get_verify_mode, __set_verify_mode)

    def __get_check_hostname(self) -> bool:
        return self._check_hostname

    def __set_check_hostname(self, value: Any) -> None:
        validate_boolean("check_hostname", value)
        self._check_hostname = value

    check_hostname = property(__get_check_hostname, __set_check_hostname)

    def __get_check_ocsp_endpoint(self) -> Optional[bool]:
        return self._callback_data.check_ocsp_endpoint

    def __set_check_ocsp_endpoint(self, value: bool) -> None:
        validate_boolean("check_ocsp", value)
        self._callback_data.check_ocsp_endpoint = value

    check_ocsp_endpoint = property(__get_check_ocsp_endpoint, __set_check_ocsp_endpoint)

    def __get_options(self) -> None:
        # Calling set_options adds the option to the existing bitmask and
        # returns the new bitmask.
        # https://www.pyopenssl.org/en/stable/api/ssl.html#OpenSSL.SSL.Context.set_options
        return self._ctx.set_options(0)

    def __set_options(self, value: int) -> None:
        # Explicitly convert to int, since newer CPython versions
        # use enum.IntFlag for options. The values are the same
        # regardless of implementation.
        self._ctx.set_options(int(value))

    options = property(__get_options, __set_options)

    def load_cert_chain(
        self,
        certfile: Union[str, bytes],
        keyfile: Union[str, bytes, None] = None,
        password: Optional[str] = None,
    ) -> None:
        """Load a private key and the corresponding certificate. The certfile
        string must be the path to a single file in PEM format containing the
        certificate as well as any number of CA certificates needed to
        establish the certificate's authenticity. The keyfile string, if
        present, must point to a file containing the private key. Otherwise
        the private key will be taken from certfile as well.
        """
        # Match CPython behavior
        # https://github.com/python/cpython/blob/v3.8.0/Modules/_ssl.c#L3930-L3971
        # Password callback MUST be set first or it will be ignored.
        if password:

            def _pwcb(_max_length: int, _prompt_twice: bool, _user_data: bytes) -> bytes:
                # XXX:We could check the password length against what OpenSSL
                # tells us is the max, but we can't raise an exception, so...
                # warn?
                assert password is not None
                return password.encode("utf-8")

            self._ctx.set_passwd_cb(_pwcb)
        self._ctx.use_certificate_chain_file(certfile)
        self._ctx.use_privatekey_file(keyfile or certfile)
        self._ctx.check_privatekey()

    def load_verify_locations(
        self, cafile: Optional[str] = None, capath: Optional[str] = None
    ) -> None:
        """Load a set of "certification authority"(CA) certificates used to
        validate other peers' certificates when `~verify_mode` is other than
        ssl.CERT_NONE.
        """
        self._ctx.load_verify_locations(cafile, capath)
        # Manually load the CA certs when get_verified_chain is not available (pyopenssl<20).
        if not hasattr(_SSL.Connection, "get_verified_chain"):
            assert cafile is not None
            self._callback_data.trusted_ca_certs = _load_trusted_ca_certs(cafile)

    def _load_certifi(self) -> None:
        """Attempt to load CA certs from certifi."""
        if _HAVE_CERTIFI:
            self.load_verify_locations(certifi.where())
        else:
            raise _ConfigurationError(
                "tlsAllowInvalidCertificates is False but no system "
                "CA certificates could be loaded. Please install the "
                "certifi package, or provide a path to a CA file using "
                "the tlsCAFile option"
            )

    def _load_wincerts(self, store: str) -> None:
        """Attempt to load CA certs from Windows trust store."""
        cert_store = self._ctx.get_cert_store()
        oid = _stdlibssl.Purpose.SERVER_AUTH.oid

        for cert, encoding, trust in _stdlibssl.enum_certificates(store):  # type: ignore
            if encoding == "x509_asn":
                if trust is True or oid in trust:
                    cert_store.add_cert(
                        _crypto.X509.from_cryptography(x509.load_der_x509_certificate(cert))
                    )

    def load_default_certs(self) -> None:
        """A PyOpenSSL version of load_default_certs from CPython."""
        # PyOpenSSL is incapable of loading CA certs from Windows, and mostly
        # incapable on macOS.
        # https://www.pyopenssl.org/en/stable/api/ssl.html#OpenSSL.SSL.Context.set_default_verify_paths
        if _sys.platform == "win32":
            try:
                for storename in ("CA", "ROOT"):
                    self._load_wincerts(storename)
            except PermissionError:
                # Fall back to certifi
                self._load_certifi()
        elif _sys.platform == "darwin":
            self._load_certifi()
        self._ctx.set_default_verify_paths()

    def set_default_verify_paths(self) -> None:
        """Specify that the platform provided CA certificates are to be used
        for verification purposes.
        """
        # Note: See PyOpenSSL's docs for limitations, which are similar
        # but not that same as CPython's.
        self._ctx.set_default_verify_paths()

    def wrap_socket(
        self,
        sock: _socket.socket,
        server_side: bool = False,
        do_handshake_on_connect: bool = True,
        suppress_ragged_eofs: bool = True,
        server_hostname: Optional[str] = None,
        session: Optional[_SSL.Session] = None,
    ) -> _sslConn:
        """Wrap an existing Python socket connection and return a TLS socket
        object.
        """
        ssl_conn = _sslConn(self._ctx, sock, suppress_ragged_eofs)
        if session:
            ssl_conn.set_session(session)
        if server_side is True:
            ssl_conn.set_accept_state()
        else:
            # SNI
            if server_hostname and not _is_ip_address(server_hostname):
                # XXX: Do this in a callback registered with
                # SSLContext.set_info_callback? See Twisted for an example.
                ssl_conn.set_tlsext_host_name(server_hostname.encode("idna"))
            if self.verify_mode != _stdlibssl.CERT_NONE:
                # Request a stapled OCSP response.
                ssl_conn.request_ocsp()
            ssl_conn.set_connect_state()
        # If this wasn't true the caller of wrap_socket would call
        # do_handshake()
        if do_handshake_on_connect:
            # XXX: If we do hostname checking in a callback we can get rid
            # of this call to do_handshake() since the handshake
            # will happen automatically later.
            ssl_conn.do_handshake()
            # XXX: Do this in a callback registered with
            # SSLContext.set_info_callback? See Twisted for an example.
            if self.check_hostname and server_hostname is not None:
                from service_identity import pyopenssl

                try:
                    if _is_ip_address(server_hostname):
                        pyopenssl.verify_ip_address(ssl_conn, server_hostname)
                    else:
                        pyopenssl.verify_hostname(ssl_conn, server_hostname)
                except (  # type:ignore[misc]
                    service_identity.SICertificateError,
                    service_identity.SIVerificationError,
                ) as exc:
                    raise _CertificateError(str(exc)) from None
        return ssl_conn
