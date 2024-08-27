from urllib.parse import urlparse
import socket
import ipaddress
from mindsdb.utilities.config import Config


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


def is_allowed_host(url):
    """
    Checks if the provided URL is from an allowed host.

    This function parses the URL and checks the network location part (netloc)
    against a list of allowed hosts.

    :param url: The URL to check.
    :return bool:  True if the URL is from an allowed host, False otherwise.
    """
    config = Config()
    parsed_url = urlparse(url)
    return parsed_url.netloc in config.get('file_upload_domains', [])


def validate_crawling_sites(urls):
    """
    Checks if the provided URL is from one of the allowed sites for web crawling.

    :param urls: The list of URLs to check.
    :return bool: True if the URL is from one of the allowed sites, False otherwise.
    """
    config = Config()
    allowed_urls = config.get('web_crawling_allowed_sites', [])
    allowed_netlocs = [urlparse(allowed_url).netloc for allowed_url in allowed_urls]
    if not allowed_urls:
        return True
    if isinstance(urls, str):
        urls = [urls]
    # Check if all provided URLs are from the allowed sites
    valid = all(urlparse(url).netloc in allowed_netlocs for url in urls)
    return valid
