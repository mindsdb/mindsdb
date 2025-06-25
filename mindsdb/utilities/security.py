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


def _split_url(url: str) -> tuple[str, str]:
    """
    Splits the URL into scheme and netloc.

    Args:
        url (str): The URL to split.

    Returns:
        tuple[str, str]: The scheme and netloc of the URL.

    Raises:
        ValueError: If the URL does not include protocol and host name.
    """
    parsed_url = urlparse(url)
    if not (parsed_url.scheme and parsed_url.netloc):
        raise ValueError(f"URL must include protocol and host name: {url}")
    return parsed_url.scheme.lower(), parsed_url.netloc.lower()


def validate_urls(urls: str | list[str], allowed_urls: list[str]) -> bool:
    """
    Checks if the provided URL(s) is/are from an allowed host.

    This function parses the URL(s) and checks the origin (scheme + netloc)
    against a list of allowed hosts.

    Examples:
        validate_urls("http://site.com/file", ["site.com"]) -> Exception
        validate_urls("https://site.com/file", ["https://site.com"]) -> True
        validate_urls("http://site.com/file", ["https://site.com"]) -> False
        validate_urls("https://site.com/file", ["https://example.com"]) -> False
        validate_urls("site.com/file", ["https://site.com"]) -> Exception

    Args:
        urls (str | list[str]): The URL(s) to check. Can be a single URL (str) or a list of URLs (list).
        allowed_urls (list[str]): The list of allowed URLs.

    Returns:
        bool: True if the URL(s) is/are from an allowed host, False otherwise.
    """
    if len(allowed_urls) == 0:
        return True

    allowed_origins = [_split_url(url) for url in allowed_urls]

    if isinstance(urls, str):
        urls = [urls]

    # Check if all provided URLs are from the allowed sites
    for url in urls:
        if _split_url(url) not in allowed_origins:
            return False
    return True
