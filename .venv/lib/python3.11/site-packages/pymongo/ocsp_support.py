# Copyright 2020-present MongoDB, Inc.
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

"""Support for requesting and verifying OCSP responses."""
from __future__ import annotations

import logging as _logging
import re as _re
from datetime import datetime as _datetime
from datetime import timezone
from typing import TYPE_CHECKING, Iterable, Optional, Type, Union

from cryptography.exceptions import InvalidSignature as _InvalidSignature
from cryptography.hazmat.backends import default_backend as _default_backend
from cryptography.hazmat.primitives.asymmetric.dsa import DSAPublicKey as _DSAPublicKey
from cryptography.hazmat.primitives.asymmetric.ec import ECDSA as _ECDSA
from cryptography.hazmat.primitives.asymmetric.ec import (
    EllipticCurvePublicKey as _EllipticCurvePublicKey,
)
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15 as _PKCS1v15
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey as _RSAPublicKey
from cryptography.hazmat.primitives.asymmetric.x448 import (
    X448PublicKey as _X448PublicKey,
)
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PublicKey as _X25519PublicKey,
)
from cryptography.hazmat.primitives.hashes import SHA1 as _SHA1
from cryptography.hazmat.primitives.hashes import Hash as _Hash
from cryptography.hazmat.primitives.serialization import Encoding as _Encoding
from cryptography.hazmat.primitives.serialization import PublicFormat as _PublicFormat
from cryptography.x509 import AuthorityInformationAccess as _AuthorityInformationAccess
from cryptography.x509 import ExtendedKeyUsage as _ExtendedKeyUsage
from cryptography.x509 import ExtensionNotFound as _ExtensionNotFound
from cryptography.x509 import TLSFeature as _TLSFeature
from cryptography.x509 import TLSFeatureType as _TLSFeatureType
from cryptography.x509 import load_pem_x509_certificate as _load_pem_x509_certificate
from cryptography.x509.ocsp import OCSPCertStatus as _OCSPCertStatus
from cryptography.x509.ocsp import OCSPRequestBuilder as _OCSPRequestBuilder
from cryptography.x509.ocsp import OCSPResponseStatus as _OCSPResponseStatus
from cryptography.x509.ocsp import load_der_ocsp_response as _load_der_ocsp_response
from cryptography.x509.oid import (
    AuthorityInformationAccessOID as _AuthorityInformationAccessOID,
)
from cryptography.x509.oid import ExtendedKeyUsageOID as _ExtendedKeyUsageOID
from requests import post as _post
from requests.exceptions import RequestException as _RequestException

from pymongo import _csot

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric import (
        dsa,
        ec,
        ed448,
        ed25519,
        rsa,
        x448,
        x25519,
    )
    from cryptography.hazmat.primitives.asymmetric.utils import Prehashed
    from cryptography.hazmat.primitives.hashes import HashAlgorithm
    from cryptography.x509 import Certificate, Name
    from cryptography.x509.extensions import Extension, ExtensionTypeVar
    from cryptography.x509.ocsp import OCSPRequest, OCSPResponse
    from OpenSSL.SSL import Connection

    from pymongo.ocsp_cache import _OCSPCache
    from pymongo.pyopenssl_context import _CallbackData

    CertificateIssuerPublicKeyTypes = Union[
        dsa.DSAPublicKey,
        rsa.RSAPublicKey,
        ec.EllipticCurvePublicKey,
        ed25519.Ed25519PublicKey,
        ed448.Ed448PublicKey,
        x25519.X25519PublicKey,
        x448.X448PublicKey,
    ]

# Note: the functions in this module generally return 1 or 0. The reason
# is simple. The entry point, ocsp_callback, is registered as a callback
# with OpenSSL through PyOpenSSL. The callback must return 1 (success) or
# 0 (failure).

_LOGGER = _logging.getLogger(__name__)

_CERT_REGEX = _re.compile(
    b"-----BEGIN CERTIFICATE[^\r\n]+.+?-----END CERTIFICATE[^\r\n]+", _re.DOTALL
)


def _load_trusted_ca_certs(cafile: str) -> list[Certificate]:
    """Parse the tlsCAFile into a list of certificates."""
    with open(cafile, "rb") as f:
        data = f.read()

    # Load all the certs in the file.
    trusted_ca_certs = []
    backend = _default_backend()
    for cert_data in _re.findall(_CERT_REGEX, data):
        trusted_ca_certs.append(_load_pem_x509_certificate(cert_data, backend))
    return trusted_ca_certs


def _get_issuer_cert(
    cert: Certificate, chain: Iterable[Certificate], trusted_ca_certs: Optional[list[Certificate]]
) -> Optional[Certificate]:
    issuer_name = cert.issuer
    for candidate in chain:
        if candidate.subject == issuer_name:
            return candidate

    # Depending on the server's TLS library, the peer's cert chain may not
    # include the self signed root CA. In this case we check the user
    # provided tlsCAFile for the issuer.
    # Remove once we use the verified peer cert chain in PYTHON-2147.
    if trusted_ca_certs:
        for candidate in trusted_ca_certs:
            if candidate.subject == issuer_name:
                return candidate
    return None


def _verify_signature(
    key: CertificateIssuerPublicKeyTypes,
    signature: bytes,
    algorithm: Union[Prehashed, HashAlgorithm, None],
    data: bytes,
) -> int:
    # See cryptography.x509.Certificate.public_key
    # for the public key types.
    try:
        if isinstance(key, _RSAPublicKey):
            key.verify(signature, data, _PKCS1v15(), algorithm)  # type: ignore[arg-type]
        elif isinstance(key, _DSAPublicKey):
            key.verify(signature, data, algorithm)  # type: ignore[arg-type]
        elif isinstance(key, _EllipticCurvePublicKey):
            key.verify(signature, data, _ECDSA(algorithm))  # type: ignore[arg-type]
        elif isinstance(
            key, (_X25519PublicKey, _X448PublicKey)
        ):  # Curve25519 and Curve448 keys do not require verification
            return 1
        else:
            key.verify(signature, data)
    except _InvalidSignature:
        return 0
    return 1


def _get_extension(
    cert: Certificate, klass: Type[ExtensionTypeVar]
) -> Optional[Extension[ExtensionTypeVar]]:
    try:
        return cert.extensions.get_extension_for_class(klass)
    except _ExtensionNotFound:
        return None


def _public_key_hash(cert: Certificate) -> bytes:
    public_key = cert.public_key()
    # https://tools.ietf.org/html/rfc2560#section-4.2.1
    # "KeyHash ::= OCTET STRING -- SHA-1 hash of responder's public key
    # (excluding the tag and length fields)"
    # https://stackoverflow.com/a/46309453/600498
    if isinstance(public_key, _RSAPublicKey):
        pbytes = public_key.public_bytes(_Encoding.DER, _PublicFormat.PKCS1)
    elif isinstance(public_key, _EllipticCurvePublicKey):
        pbytes = public_key.public_bytes(_Encoding.X962, _PublicFormat.UncompressedPoint)
    else:
        pbytes = public_key.public_bytes(_Encoding.DER, _PublicFormat.SubjectPublicKeyInfo)
    digest = _Hash(_SHA1(), backend=_default_backend())  # noqa: S303
    digest.update(pbytes)
    return digest.finalize()


def _get_certs_by_key_hash(
    certificates: Iterable[Certificate], issuer: Certificate, responder_key_hash: Optional[bytes]
) -> list[Certificate]:
    return [
        cert
        for cert in certificates
        if _public_key_hash(cert) == responder_key_hash and cert.issuer == issuer.subject
    ]


def _get_certs_by_name(
    certificates: Iterable[Certificate], issuer: Certificate, responder_name: Optional[Name]
) -> list[Certificate]:
    return [
        cert
        for cert in certificates
        if cert.subject == responder_name and cert.issuer == issuer.subject
    ]


def _verify_response_signature(issuer: Certificate, response: OCSPResponse) -> int:
    # Response object will have a responder_name or responder_key_hash
    # not both.
    name = response.responder_name
    rkey_hash = response.responder_key_hash
    ikey_hash = response.issuer_key_hash
    if name is not None and name == issuer.subject or rkey_hash == ikey_hash:
        _LOGGER.debug("Responder is issuer")
        # Responder is the issuer
        responder_cert = issuer
    else:
        _LOGGER.debug("Responder is a delegate")
        # Responder is a delegate
        # https://tools.ietf.org/html/rfc6960#section-2.6
        # RFC6960, Section 3.2, Number 3
        certs = response.certificates
        if response.responder_name is not None:
            responder_certs = _get_certs_by_name(certs, issuer, name)
            _LOGGER.debug("Using responder name")
        else:
            responder_certs = _get_certs_by_key_hash(certs, issuer, rkey_hash)
            _LOGGER.debug("Using key hash")
        if not responder_certs:
            _LOGGER.debug("No matching or valid responder certs.")
            return 0
        # XXX: Can there be more than one? If so, should we try each one
        # until we find one that passes signature verification?
        responder_cert = responder_certs[0]

        # RFC6960, Section 3.2, Number 4
        ext = _get_extension(responder_cert, _ExtendedKeyUsage)
        if not ext or _ExtendedKeyUsageOID.OCSP_SIGNING not in ext.value:
            _LOGGER.debug("Delegate not authorized for OCSP signing")
            return 0
        if not _verify_signature(
            issuer.public_key(),
            responder_cert.signature,
            responder_cert.signature_hash_algorithm,
            responder_cert.tbs_certificate_bytes,
        ):
            _LOGGER.debug("Delegate signature verification failed")
            return 0
    # RFC6960, Section 3.2, Number 2
    ret = _verify_signature(
        responder_cert.public_key(),
        response.signature,
        response.signature_hash_algorithm,
        response.tbs_response_bytes,
    )
    if not ret:
        _LOGGER.debug("Response signature verification failed")
    return ret


def _build_ocsp_request(cert: Certificate, issuer: Certificate) -> OCSPRequest:
    # https://cryptography.io/en/latest/x509/ocsp/#creating-requests
    builder = _OCSPRequestBuilder()
    builder = builder.add_certificate(cert, issuer, _SHA1())  # noqa: S303
    return builder.build()


def _verify_response(issuer: Certificate, response: OCSPResponse) -> int:
    _LOGGER.debug("Verifying response")
    # RFC6960, Section 3.2, Number 2, 3 and 4 happen here.
    res = _verify_response_signature(issuer, response)
    if not res:
        return 0

    # Note that we are not using a "tolerance period" as discussed in
    # https://tools.ietf.org/rfc/rfc5019.txt?
    now = _datetime.now(tz=timezone.utc).replace(tzinfo=None)
    # RFC6960, Section 3.2, Number 5
    if response.this_update > now:
        _LOGGER.debug("thisUpdate is in the future")
        return 0
    # RFC6960, Section 3.2, Number 6
    if response.next_update and response.next_update < now:
        _LOGGER.debug("nextUpdate is in the past")
        return 0
    return 1


def _get_ocsp_response(
    cert: Certificate, issuer: Certificate, uri: Union[str, bytes], ocsp_response_cache: _OCSPCache
) -> Optional[OCSPResponse]:
    ocsp_request = _build_ocsp_request(cert, issuer)
    try:
        ocsp_response = ocsp_response_cache[ocsp_request]
        _LOGGER.debug("Using cached OCSP response.")
    except KeyError:
        # CSOT: use the configured timeout or 5 seconds, whichever is smaller.
        # Note that request's timeout works differently and does not imply an absolute
        # deadline: https://requests.readthedocs.io/en/stable/user/quickstart/#timeouts
        timeout = max(_csot.clamp_remaining(5), 0.001)
        try:
            response = _post(
                uri,
                data=ocsp_request.public_bytes(_Encoding.DER),
                headers={"Content-Type": "application/ocsp-request"},
                timeout=timeout,
            )
        except _RequestException as exc:
            _LOGGER.debug("HTTP request failed: %s", exc)
            return None
        if response.status_code != 200:
            _LOGGER.debug("HTTP request returned %d", response.status_code)
            return None
        ocsp_response = _load_der_ocsp_response(response.content)
        _LOGGER.debug("OCSP response status: %r", ocsp_response.response_status)
        if ocsp_response.response_status != _OCSPResponseStatus.SUCCESSFUL:
            return None
        # RFC6960, Section 3.2, Number 1. Only relevant if we need to
        # talk to the responder directly.
        # Accessing response.serial_number raises if response status is not
        # SUCCESSFUL.
        if ocsp_response.serial_number != ocsp_request.serial_number:
            _LOGGER.debug("Response serial number does not match request")
            return None
        if not _verify_response(issuer, ocsp_response):
            # The response failed verification.
            return None
        _LOGGER.debug("Caching OCSP response.")
        ocsp_response_cache[ocsp_request] = ocsp_response

    return ocsp_response


def _ocsp_callback(conn: Connection, ocsp_bytes: bytes, user_data: Optional[_CallbackData]) -> bool:
    """Callback for use with OpenSSL.SSL.Context.set_ocsp_client_callback."""
    # always pass in user_data but OpenSSL requires it be optional
    assert user_data
    pycert = conn.get_peer_certificate()
    if pycert is None:
        _LOGGER.debug("No peer cert?")
        return False
    cert = pycert.to_cryptography()
    # Use the verified chain when available (pyopenssl>=20.0).
    if hasattr(conn, "get_verified_chain"):
        pychain = conn.get_verified_chain()
        trusted_ca_certs = None
    else:
        pychain = conn.get_peer_cert_chain()
        trusted_ca_certs = user_data.trusted_ca_certs
    if not pychain:
        _LOGGER.debug("No peer cert chain?")
        return False
    chain = [cer.to_cryptography() for cer in pychain]
    issuer = _get_issuer_cert(cert, chain, trusted_ca_certs)
    must_staple = False
    # https://tools.ietf.org/html/rfc7633#section-4.2.3.1
    ext_tls = _get_extension(cert, _TLSFeature)
    if ext_tls is not None:
        for feature in ext_tls.value:
            if feature == _TLSFeatureType.status_request:
                _LOGGER.debug("Peer presented a must-staple cert")
                must_staple = True
                break
    ocsp_response_cache = user_data.ocsp_response_cache

    # No stapled OCSP response
    if ocsp_bytes == b"":
        _LOGGER.debug("Peer did not staple an OCSP response")
        if must_staple:
            _LOGGER.debug("Must-staple cert with no stapled response, hard fail.")
            return False
        if not user_data.check_ocsp_endpoint:
            _LOGGER.debug("OCSP endpoint checking is disabled, soft fail.")
            # No stapled OCSP response, checking responder URI disabled, soft fail.
            return True
        # https://tools.ietf.org/html/rfc6960#section-3.1
        ext_aia = _get_extension(cert, _AuthorityInformationAccess)
        if ext_aia is None:
            _LOGGER.debug("No authority access information, soft fail")
            # No stapled OCSP response, no responder URI, soft fail.
            return True
        uris = [
            desc.access_location.value
            for desc in ext_aia.value
            if desc.access_method == _AuthorityInformationAccessOID.OCSP
        ]
        if not uris:
            _LOGGER.debug("No OCSP URI, soft fail")
            # No responder URI, soft fail.
            return True
        if issuer is None:
            _LOGGER.debug("No issuer cert?")
            return False
        _LOGGER.debug("Requesting OCSP data")
        # When requesting data from an OCSP endpoint we only fail on
        # successful, valid responses with a certificate status of REVOKED.
        for uri in uris:
            _LOGGER.debug("Trying %s", uri)
            response = _get_ocsp_response(cert, issuer, uri, ocsp_response_cache)
            if response is None:
                # The endpoint didn't respond in time, or the response was
                # unsuccessful or didn't match the request, or the response
                # failed verification.
                continue
            _LOGGER.debug("OCSP cert status: %r", response.certificate_status)
            if response.certificate_status == _OCSPCertStatus.GOOD:
                return True
            if response.certificate_status == _OCSPCertStatus.REVOKED:
                return False
        # Soft fail if we couldn't get a definitive status.
        _LOGGER.debug("No definitive OCSP cert status, soft fail")
        return True

    _LOGGER.debug("Peer stapled an OCSP response")
    if issuer is None:
        _LOGGER.debug("No issuer cert?")
        return False
    response = _load_der_ocsp_response(ocsp_bytes)
    _LOGGER.debug("OCSP response status: %r", response.response_status)
    # This happens in _request_ocsp when there is no stapled response so
    # we know if we can compare serial numbers for the request and response.
    if response.response_status != _OCSPResponseStatus.SUCCESSFUL:
        return False
    if not _verify_response(issuer, response):
        return False
    # Cache the verified, stapled response.
    ocsp_response_cache[_build_ocsp_request(cert, issuer)] = response
    _LOGGER.debug("OCSP cert status: %r", response.certificate_status)
    if response.certificate_status == _OCSPCertStatus.REVOKED:
        return False
    return True
