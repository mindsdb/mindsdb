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


def validate_urls(urls, allowed_urls):
    """
    Checks if the provided URL(s) is/are from an allowed host.

    This function parses the URL(s) and checks the network location part (netloc)
    against a list of allowed hosts.

    :param urls: The URL(s) to check. Can be a single URL (str) or a list of URLs (list).
    :param allowed_urls: The list of allowed URLs.
    :return bool:  True if the URL(s) is/are from an allowed host, False otherwise.
    """
    allowed_netlocs = [urlparse(allowed_url).netloc for allowed_url in allowed_urls]

    if isinstance(urls, str):
        urls = [urls]

    # Check if all provided URLs are from the allowed sites
    valid = all(urlparse(url).netloc in allowed_netlocs for url in urls)
    return valid
