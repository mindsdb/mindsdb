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


def encrypt_dict(message: dict) -> dict:
    """Encrypt dict using key in context

    Args:
        message (dict): dict to encrypt

    Returns:
        dict: dcit containing a key (_mindsdb_encrypted_data) with encrypted data
    """

    key = ctx.encryption_key
    if key is None or len(key) == 0:
        if Config().get('cloud', False) is False:
            logger.error(f"Encryption key is not found for company_id={ctx.company_id}")
            raise Exception("Encryption key cannot be found")
        return message

    nonce = secrets.token_bytes(12)
    message_bytes = pickle.dumps(message)  # json.dumps(message).encode()
    encrypted_bytes = nonce + AESGCM(key).encrypt(nonce, message_bytes, None)
    encrypted_bytes = base64.b64encode(encrypted_bytes).decode('utf-8')

    return {
        '_mindsdb_encrypted_data': encrypted_bytes,
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

    key = ctx.encryption_key
    if key is None or len(key) == 0:
        logger.error(f"Encryption key is not found for company_id={ctx.company_id}")
        raise Exception("Encryption key cannot be found")

    encrypted_message = base64.b64decode(encrypted_message.encode('utf-8'))
    decrypted_message = AESGCM(key).decrypt(encrypted_message[:12], encrypted_message[12:], None)
    decrypted_message = pickle.loads(decrypted_message)
    return decrypted_message
