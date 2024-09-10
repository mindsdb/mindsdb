from typing import Union
from urllib.parse import urlparse
import socket
import ipaddress
import secrets
import pickle
import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log

from mindsdb.utilities.config import Config


logger = log.getLogger(__name__)

ENCRYPT_PREFIX = 'MDB\x00ENC'


def is_private_url(url: str):
    """
    Raises exception if url is private

    :param url: url to check
    """

    hostname = urlparse(url).hostname
    if not hostname:
        # Unable find hostname in url
        return True
    ip = socket.gethostbyname(hostname)
    return ipaddress.ip_address(ip).is_private


def clear_filename(filename: str) -> str:
    """
    Removes path symbols from filename which could be used for path injection
    :param filename: input filename
    :return: output filename
    """

    if not filename:
        return filename
    badchars = '\\/:*?\"<>|'
    for c in badchars:
        filename = filename.replace(c, '')
    return filename


def _encrypt_v1(data: bytes) -> str:
    protocol_version = 1

    key = ctx.encryption_key_bytes
    if key is None:
        raise Exception("Encryption key not found")

    nonce = secrets.token_bytes(12)
    encrypted_bytes = nonce + AESGCM(key).encrypt(nonce, data, None)
    encrypted_str = base64.b64encode(encrypted_bytes).decode('utf-8')
    return ENCRYPT_PREFIX + chr(protocol_version) + encrypted_str


def _decrypt(data) -> bytes:
    if not data.startswith(ENCRYPT_PREFIX):
        # not encrypted
        return data.encode('utf-8')

    key = ctx.encryption_key_bytes
    if key is None:
        raise Exception("Encryption key not found")

    offset = len(ENCRYPT_PREFIX)
    protocol_version = ord(data[offset: offset + 1])

    if protocol_version == 1:

        encrypted_message = base64.b64decode(data[offset + 1:].encode('utf-8'))
        try:
            decrypted_message = AESGCM(key).decrypt(encrypted_message[:12], encrypted_message[12:], None)
        except Exception as e:
            raise Exception("Wrong encryption key") from e
    else:
        raise ValueError(f'Encrypted object protocol version is unknown: {protocol_version}')

    return decrypted_message


def encrypt_object(data: object) -> Union[object, str]:
    """Serialize object to encrypted string.
    If encryption is not enabled it returns the same object

    Args:
        data (object): any object that can be pickled

    Returns:
        str or object: encrypted string or input object
    """
    if not Config().encryption_enabled:
        return data

    message_bytes = pickle.dumps(data)
    return _encrypt_v1(message_bytes)


def decrypt_object(data: str) -> object:
    """Deserialize string to an object

    Args:
        data (str): encrypted string

    Returns:
        object: unpickled object
    """
    decrypted_message = _decrypt(data)
    decrypted_object = pickle.loads(decrypted_message)
    return decrypted_object


def encrypt(data: str) -> str:
    """Encrypt string
    If encryption is not enabled it returns the same string

    Args:
        data (str): string to be encrypted

    Returns:
        str: encrypted string
    """
    if not Config().encryption_enabled:
        return data

    message_bytes = data.encode('utf-8')
    return _encrypt_v1(message_bytes)


def decrypt(data: str) -> str:
    """Decrypt string

    Args:
        data (str): encrypted string

    Returns:
        str: decrypted string
    """

    decrypted_message = _decrypt(data)
    decrypted_str = decrypted_message.decode()
    return decrypted_str
