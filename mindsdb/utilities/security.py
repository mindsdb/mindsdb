from urllib.parse import urlparse
import socket
import ipaddress
import secrets
import pickle
import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log


logger = log.getLogger(__name__)


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


def encrypt(obj: object) -> dict:
    """Encrypt object

    Args:
        obj (Any): python object

    Returns:
        dict: structure with encrypted data
    """
    protocol_version = 1
    if ctx.encryption_key is None:
        raise Exception('The encryption key is missing')
    if obj is None:
        raise ValueError('Cannot encrypt None')
    if isinstance(obj, str):
        return {
            'protocol_version': protocol_version,
            'object_type': 'str',
            'encrypted_object': encrypt_str(obj)
        }
    return {
        'protocol_version': protocol_version,
        'object_type': 'object',
        'encrypted_object': encrypt_object(obj)
    }


def decrypt(encrypted_object: dict) -> object:
    """Decrypt object

    Args:
        encrypted_object (dict): structure with encrypted data

    Returns:
        object: decrypted python object
    """
    if isinstance(encrypted_object, dict) is False:
        raise ValueError('Encrypted object must be dict')
    protocol_version = encrypted_object.get('protocol_version')
    if protocol_version != 1:
        raise ValueError(f'Encrypted object protocol version is unknown: {protocol_version}')
    object_type = encrypted_object.get('object_type')
    if object_type == 'str':
        return decrypt_str(encrypted_object['encrypted_object'])
    return decrypt_object(encrypted_object['encrypted_object'])


def _encrypt(data: bytes) -> str:
    key = ctx.encryption_key_bytes
    nonce = secrets.token_bytes(12)
    encrypted_bytes = nonce + AESGCM(key).encrypt(nonce, data, None)
    encrypted_str = base64.b64encode(encrypted_bytes).decode('utf-8')
    return encrypted_str


def _decrypt(data) -> bytes:
    key = ctx.encryption_key_bytes

    encrypted_message = base64.b64decode(data.encode('utf-8'))
    try:
        decrypted_message = AESGCM(key).decrypt(encrypted_message[:12], encrypted_message[12:], None)
    except Exception as e:
        logger.error(f'Wrong encryption key: {e}')
    return decrypted_message


def encrypt_object(data: object) -> str:
    """Serialize object to encrypted string

    Args:
        data (object): any object that can be pickled

    Returns:
        str: encrypted string
    """
    message_bytes = pickle.dumps(data)
    return _encrypt(message_bytes)


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


def encrypt_str(data: str) -> str:
    """Encrypt string

    Args:
        data (str): string to be encrypted

    Returns:
        str: encrypted string
    """
    message_bytes = data.encode('utf-8')
    return _encrypt(message_bytes)


def decrypt_str(data: str) -> str:
    """Decrypt string

    Args:
        data (str): encrypted string

    Returns:
        str: decrypted string
    """
    key = ctx.encryption_key_bytes
    if key is None or len(key) == 0:
        logger.error(f"Encryption key is not found for company_id={ctx.company_id}")
        raise Exception("Encryption key cannot be found")

    decrypted_message = _decrypt(data)
    decrypted_str = decrypted_message.decode()
    return decrypted_str
