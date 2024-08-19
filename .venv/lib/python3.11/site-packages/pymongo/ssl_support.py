# Copyright 2014-present MongoDB, Inc.
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

"""Support for SSL in PyMongo."""
from __future__ import annotations

import warnings
from typing import Optional

from pymongo.errors import ConfigurationError

HAVE_SSL = True

try:
    import pymongo.pyopenssl_context as _ssl
except (ImportError, AttributeError) as exc:
    if isinstance(exc, AttributeError):
        warnings.warn(
            "Failed to use the installed version of PyOpenSSL. "
            "Falling back to stdlib ssl, disabling OCSP support. "
            "This is likely caused by incompatible versions "
            "of PyOpenSSL < 23.2.0 and cryptography >= 42.0.0. "
            "Try updating PyOpenSSL >= 23.2.0 to enable OCSP.",
            UserWarning,
            stacklevel=2,
        )
    try:
        import pymongo.ssl_context as _ssl  # type: ignore[no-redef]
    except ImportError:
        HAVE_SSL = False


if HAVE_SSL:
    # Note: The validate* functions below deal with users passing
    # CPython ssl module constants to configure certificate verification
    # at a high level. This is legacy behavior, but requires us to
    # import the ssl module even if we're only using it for this purpose.
    import ssl as _stdlibssl  # noqa: F401
    from ssl import CERT_NONE, CERT_REQUIRED

    HAS_SNI = _ssl.HAS_SNI
    IPADDR_SAFE = True
    SSLError = _ssl.SSLError
    BLOCKING_IO_ERRORS = _ssl.BLOCKING_IO_ERRORS

    def get_ssl_context(
        certfile: Optional[str],
        passphrase: Optional[str],
        ca_certs: Optional[str],
        crlfile: Optional[str],
        allow_invalid_certificates: bool,
        allow_invalid_hostnames: bool,
        disable_ocsp_endpoint_check: bool,
    ) -> _ssl.SSLContext:
        """Create and return an SSLContext object."""
        verify_mode = CERT_NONE if allow_invalid_certificates else CERT_REQUIRED
        ctx = _ssl.SSLContext(_ssl.PROTOCOL_SSLv23)
        if verify_mode != CERT_NONE:
            ctx.check_hostname = not allow_invalid_hostnames
        else:
            ctx.check_hostname = False
        if hasattr(ctx, "check_ocsp_endpoint"):
            ctx.check_ocsp_endpoint = not disable_ocsp_endpoint_check
        if hasattr(ctx, "options"):
            # Explicitly disable SSLv2, SSLv3 and TLS compression. Note that
            # up to date versions of MongoDB 2.4 and above already disable
            # SSLv2 and SSLv3, python disables SSLv2 by default in >= 2.7.7
            # and >= 3.3.4 and SSLv3 in >= 3.4.3.
            ctx.options |= _ssl.OP_NO_SSLv2
            ctx.options |= _ssl.OP_NO_SSLv3
            ctx.options |= _ssl.OP_NO_COMPRESSION
            ctx.options |= _ssl.OP_NO_RENEGOTIATION
        if certfile is not None:
            try:
                ctx.load_cert_chain(certfile, None, passphrase)
            except _ssl.SSLError as exc:
                raise ConfigurationError(f"Private key doesn't match certificate: {exc}") from None
        if crlfile is not None:
            if _ssl.IS_PYOPENSSL:
                raise ConfigurationError("tlsCRLFile cannot be used with PyOpenSSL")
            # Match the server's behavior.
            ctx.verify_flags = getattr(  # type:ignore[attr-defined]
                _ssl, "VERIFY_CRL_CHECK_LEAF", 0
            )
            ctx.load_verify_locations(crlfile)
        if ca_certs is not None:
            ctx.load_verify_locations(ca_certs)
        elif verify_mode != CERT_NONE:
            ctx.load_default_certs()
        ctx.verify_mode = verify_mode
        return ctx

else:

    class SSLError(Exception):  # type: ignore
        pass

    HAS_SNI = False
    IPADDR_SAFE = False
    BLOCKING_IO_ERRORS = ()  # type:ignore[assignment]

    def get_ssl_context(*dummy):  # type: ignore
        """No ssl module, raise ConfigurationError."""
        raise ConfigurationError("The ssl module is not available.")
