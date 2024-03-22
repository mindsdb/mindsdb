from urllib.parse import urlparse
import socket
import ipaddress
import secrets
import pickle
import base64
from copy import deepcopy

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.config import Config
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


def encrypt_dict(message: dict) -> dict:
    """Encrypt dict using key in context

    Args:
        message (dict): dict to encrypt

    Returns:
        dict: dcit containing a key (_mindsdb_encrypted_data) with encrypted data
    """
    encrypted_str = encrypt_object(message)
    return {
        '_mindsdb_encrypted_data': encrypted_str,
        'keys': list(message.keys())
    }


def decrypt_dict(message: dict) -> dict:
    """Decrypt dict using key in context

    Args:
        message (dict): dict to decrypt, must have `_mindsdb_encrypted_data` key

    Returns:
        dict: decrypted dict
    """
    encrypted_message = message.get('_mindsdb_encrypted_data')

    if encrypted_message is None:
        if Config().get('cloud', False) is False:
            logger.warn(f"Data is not encrypted for company_id={ctx.company_id}")
        return deepcopy(message)

    decrypted_dict = decrypt_object(encrypted_message)
    return decrypted_dict


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
