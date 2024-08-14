from urllib.parse import urlparse
import socket
import ipaddress


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


# TODO: make this list configurable in config.json
ALLOWED_DOMAINS = [
    "s3.amazonaws.com",
    "drive.google.com",
    "blob.core.windows.net",
    "dropbox.com",
    "githubusercontent.com",
    "onedrive.live.com"
]


def is_allowed_url(url):
    """
    Checks if the provided URL is from an allowed host.

    This function parses the URL and checks the network location part (netloc)
    against a list of allowed hosts.

    :param url: The URL to check.
    :return bool:  True if the URL is from an allowed host, False otherwise.
    """
    parsed_url = urlparse(url.lower())
    return parsed_url.netloc in ALLOWED_DOMAINS
