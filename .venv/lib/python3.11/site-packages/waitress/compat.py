import platform

# Fix for issue reported in https://github.com/Pylons/waitress/issues/138,
# Python on Windows may not define IPPROTO_IPV6 in socket.
import socket
import sys
import warnings

# True if we are running on Windows
WIN = platform.system() == "Windows"

MAXINT = sys.maxsize
HAS_IPV6 = socket.has_ipv6

if hasattr(socket, "IPPROTO_IPV6") and hasattr(socket, "IPV6_V6ONLY"):
    IPPROTO_IPV6 = socket.IPPROTO_IPV6
    IPV6_V6ONLY = socket.IPV6_V6ONLY
else:  # pragma: no cover
    if WIN:
        IPPROTO_IPV6 = 41
        IPV6_V6ONLY = 27
    else:
        warnings.warn(
            "OS does not support required IPv6 socket flags. This is requirement "
            "for Waitress. Please open an issue at https://github.com/Pylons/waitress. "
            "IPv6 support has been disabled.",
            RuntimeWarning,
        )
        HAS_IPV6 = False
