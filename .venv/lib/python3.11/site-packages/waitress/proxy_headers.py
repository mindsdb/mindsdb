from collections import namedtuple

from .utilities import BadRequest, logger, undquote

PROXY_HEADERS = frozenset(
    {
        "X_FORWARDED_FOR",
        "X_FORWARDED_HOST",
        "X_FORWARDED_PROTO",
        "X_FORWARDED_PORT",
        "X_FORWARDED_BY",
        "FORWARDED",
    }
)

Forwarded = namedtuple("Forwarded", ["by", "for_", "host", "proto"])


class MalformedProxyHeader(Exception):
    def __init__(self, header, reason, value):
        self.header = header
        self.reason = reason
        self.value = value
        super().__init__(header, reason, value)


def proxy_headers_middleware(
    app,
    trusted_proxy=None,
    trusted_proxy_count=1,
    trusted_proxy_headers=None,
    clear_untrusted=True,
    log_untrusted=False,
    logger=logger,
):
    def translate_proxy_headers(environ, start_response):
        untrusted_headers = PROXY_HEADERS
        remote_peer = environ["REMOTE_ADDR"]
        if trusted_proxy == "*" or remote_peer == trusted_proxy:
            try:
                untrusted_headers = parse_proxy_headers(
                    environ,
                    trusted_proxy_count=trusted_proxy_count,
                    trusted_proxy_headers=trusted_proxy_headers,
                    logger=logger,
                )
            except MalformedProxyHeader as ex:
                logger.warning(
                    'Malformed proxy header "%s" from "%s": %s value: %s',
                    ex.header,
                    remote_peer,
                    ex.reason,
                    ex.value,
                )
                error = BadRequest(f'Header "{ex.header}" malformed.')
                return error.wsgi_response(environ, start_response)

        # Clear out the untrusted proxy headers
        if clear_untrusted:
            clear_untrusted_headers(
                environ, untrusted_headers, log_warning=log_untrusted, logger=logger
            )

        return app(environ, start_response)

    return translate_proxy_headers


def parse_proxy_headers(
    environ, trusted_proxy_count, trusted_proxy_headers, logger=logger
):
    if trusted_proxy_headers is None:
        trusted_proxy_headers = set()

    forwarded_for = []
    forwarded_host = forwarded_proto = forwarded_port = forwarded = ""
    client_addr = None
    untrusted_headers = set(PROXY_HEADERS)

    def raise_for_multiple_values():
        raise ValueError("Unspecified behavior for multiple values found in header")

    if "x-forwarded-for" in trusted_proxy_headers and "HTTP_X_FORWARDED_FOR" in environ:
        try:
            forwarded_for = []

            for forward_hop in environ["HTTP_X_FORWARDED_FOR"].split(","):
                forward_hop = forward_hop.strip()
                forward_hop = undquote(forward_hop)

                # Make sure that all IPv6 addresses are surrounded by brackets,
                # this is assuming that the IPv6 representation here does not
                # include a port number.

                if "." not in forward_hop and (
                    ":" in forward_hop and forward_hop[-1] != "]"
                ):
                    forwarded_for.append(f"[{forward_hop}]")
                else:
                    forwarded_for.append(forward_hop)

            forwarded_for = forwarded_for[-trusted_proxy_count:]
            client_addr = forwarded_for[0]

            untrusted_headers.remove("X_FORWARDED_FOR")
        except Exception as ex:
            raise MalformedProxyHeader(
                "X-Forwarded-For", str(ex), environ["HTTP_X_FORWARDED_FOR"]
            )

    if (
        "x-forwarded-host" in trusted_proxy_headers
        and "HTTP_X_FORWARDED_HOST" in environ
    ):
        try:
            forwarded_host_multiple = []

            for forward_host in environ["HTTP_X_FORWARDED_HOST"].split(","):
                forward_host = forward_host.strip()
                forward_host = undquote(forward_host)
                forwarded_host_multiple.append(forward_host)

            forwarded_host_multiple = forwarded_host_multiple[-trusted_proxy_count:]
            forwarded_host = forwarded_host_multiple[0]

            untrusted_headers.remove("X_FORWARDED_HOST")
        except Exception as ex:
            raise MalformedProxyHeader(
                "X-Forwarded-Host", str(ex), environ["HTTP_X_FORWARDED_HOST"]
            )

    if "x-forwarded-proto" in trusted_proxy_headers:
        try:
            forwarded_proto = undquote(environ.get("HTTP_X_FORWARDED_PROTO", ""))
            if "," in forwarded_proto:
                raise_for_multiple_values()
            untrusted_headers.remove("X_FORWARDED_PROTO")
        except Exception as ex:
            raise MalformedProxyHeader(
                "X-Forwarded-Proto", str(ex), environ["HTTP_X_FORWARDED_PROTO"]
            )

    if "x-forwarded-port" in trusted_proxy_headers:
        try:
            forwarded_port = undquote(environ.get("HTTP_X_FORWARDED_PORT", ""))
            if "," in forwarded_port:
                raise_for_multiple_values()
            untrusted_headers.remove("X_FORWARDED_PORT")
        except Exception as ex:
            raise MalformedProxyHeader(
                "X-Forwarded-Port", str(ex), environ["HTTP_X_FORWARDED_PORT"]
            )

    if "x-forwarded-by" in trusted_proxy_headers:
        # Waitress itself does not use X-Forwarded-By, but we can not
        # remove it so it can get set in the environ
        untrusted_headers.remove("X_FORWARDED_BY")

    if "forwarded" in trusted_proxy_headers:
        forwarded = environ.get("HTTP_FORWARDED", None)
        untrusted_headers = PROXY_HEADERS - {"FORWARDED"}

    # If the Forwarded header exists, it gets priority
    if forwarded:
        proxies = []
        try:
            for forwarded_element in forwarded.split(","):
                # Remove whitespace that may have been introduced when
                # appending a new entry
                forwarded_element = forwarded_element.strip()

                forwarded_for = forwarded_host = forwarded_proto = ""
                forwarded_port = forwarded_by = ""

                for pair in forwarded_element.split(";"):
                    pair = pair.lower()

                    if not pair:
                        continue

                    token, equals, value = pair.partition("=")

                    if equals != "=":
                        raise ValueError('Invalid forwarded-pair missing "="')

                    if token.strip() != token:
                        raise ValueError("Token may not be surrounded by whitespace")

                    if value.strip() != value:
                        raise ValueError("Value may not be surrounded by whitespace")

                    if token == "by":
                        forwarded_by = undquote(value)

                    elif token == "for":
                        forwarded_for = undquote(value)

                    elif token == "host":
                        forwarded_host = undquote(value)

                    elif token == "proto":
                        forwarded_proto = undquote(value)

                    else:
                        logger.warning("Unknown Forwarded token: %s" % token)

                proxies.append(
                    Forwarded(
                        forwarded_by, forwarded_for, forwarded_host, forwarded_proto
                    )
                )
        except Exception as ex:
            raise MalformedProxyHeader("Forwarded", str(ex), environ["HTTP_FORWARDED"])

        proxies = proxies[-trusted_proxy_count:]

        # Iterate backwards and fill in some values, the oldest entry that
        # contains the information we expect is the one we use. We expect
        # that intermediate proxies may re-write the host header or proto,
        # but the oldest entry is the one that contains the information the
        # client expects when generating URL's
        #
        # Forwarded: for="[2001:db8::1]";host="example.com:8443";proto="https"
        # Forwarded: for=192.0.2.1;host="example.internal:8080"
        #
        # (After HTTPS header folding) should mean that we use as values:
        #
        # Host: example.com
        # Protocol: https
        # Port: 8443

        for proxy in proxies[::-1]:
            client_addr = proxy.for_ or client_addr
            forwarded_host = proxy.host or forwarded_host
            forwarded_proto = proxy.proto or forwarded_proto

    if forwarded_proto:
        forwarded_proto = forwarded_proto.lower()

        if forwarded_proto not in {"http", "https"}:
            raise MalformedProxyHeader(
                "Forwarded Proto=" if forwarded else "X-Forwarded-Proto",
                "unsupported proto value",
                forwarded_proto,
            )

        # Set the URL scheme to the proxy provided proto
        environ["wsgi.url_scheme"] = forwarded_proto

        if not forwarded_port:
            if forwarded_proto == "http":
                forwarded_port = "80"

            if forwarded_proto == "https":
                forwarded_port = "443"

    if forwarded_host:
        if ":" in forwarded_host and forwarded_host[-1] != "]":
            host, port = forwarded_host.rsplit(":", 1)
            host, port = host.strip(), str(port)

            # We trust the port in the Forwarded Host/X-Forwarded-Host over
            # X-Forwarded-Port, or whatever we got from Forwarded
            # Proto/X-Forwarded-Proto.

            if forwarded_port != port:
                forwarded_port = port

            # We trust the proxy server's forwarded Host
            environ["SERVER_NAME"] = host
            environ["HTTP_HOST"] = forwarded_host
        else:
            # We trust the proxy server's forwarded Host
            environ["SERVER_NAME"] = forwarded_host
            environ["HTTP_HOST"] = forwarded_host

            if forwarded_port:
                if forwarded_port not in {"443", "80"}:
                    environ["HTTP_HOST"] = "{}:{}".format(
                        forwarded_host, forwarded_port
                    )
                elif forwarded_port == "80" and environ["wsgi.url_scheme"] != "http":
                    environ["HTTP_HOST"] = "{}:{}".format(
                        forwarded_host, forwarded_port
                    )
                elif forwarded_port == "443" and environ["wsgi.url_scheme"] != "https":
                    environ["HTTP_HOST"] = "{}:{}".format(
                        forwarded_host, forwarded_port
                    )

    if forwarded_port:
        environ["SERVER_PORT"] = str(forwarded_port)

    if client_addr:
        if ":" in client_addr and client_addr[-1] != "]":
            addr, port = client_addr.rsplit(":", 1)
            environ["REMOTE_ADDR"] = strip_brackets(addr.strip())
            environ["REMOTE_PORT"] = port.strip()
        else:
            environ["REMOTE_ADDR"] = strip_brackets(client_addr.strip())
        environ["REMOTE_HOST"] = environ["REMOTE_ADDR"]

    return untrusted_headers


def strip_brackets(addr):
    if addr[0] == "[" and addr[-1] == "]":
        return addr[1:-1]
    return addr


def clear_untrusted_headers(
    environ, untrusted_headers, log_warning=False, logger=logger
):
    untrusted_headers_removed = [
        header
        for header in untrusted_headers
        if environ.pop("HTTP_" + header, False) is not False
    ]

    if log_warning and untrusted_headers_removed:
        untrusted_headers_removed = [
            "-".join(x.capitalize() for x in header.split("_"))
            for header in untrusted_headers_removed
        ]
        logger.warning(
            "Removed untrusted headers (%s). Waitress recommends these be "
            "removed upstream.",
            ", ".join(untrusted_headers_removed),
        )
