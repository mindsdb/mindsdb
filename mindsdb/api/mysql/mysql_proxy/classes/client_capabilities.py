from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import CAPABILITIES


class ClentCapabilities():
    _capabilities = 0

    def __init__(self, capabilities):
        self._capabilities = capabilities

    def has(self, cap):
        return cap & self._capabilities > 0

    def to_dict(self):
        funcs = [func for func in dir(self) if func.upper() == func]
        res = {}
        for f in funcs:
            res[f] = getattr(self, f)
        return res

    @property
    def LONG_PASSWORD(self):
        return self.has(CAPABILITIES.CLIENT_LONG_PASSWORD)

    @property
    def FOUND_ROWS(self):
        return self.has(CAPABILITIES.CLIENT_FOUND_ROWS)

    @property
    def LONG_FLAG(self):
        return self.has(CAPABILITIES.CLIENT_LONG_FLAG)

    @property
    def CONNECT_WITH_DB(self):
        return self.has(CAPABILITIES.CLIENT_CONNECT_WITH_DB)

    @property
    def NO_SCHEMA(self):
        return self.has(CAPABILITIES.CLIENT_NO_SCHEMA)

    @property
    def COMPRESS(self):
        return self.has(CAPABILITIES.CLIENT_COMPRESS)

    @property
    def ODBC(self):
        return self.has(CAPABILITIES.CLIENT_ODBC)

    @property
    def LOCAL_FILES(self):
        return self.has(CAPABILITIES.CLIENT_LOCAL_FILES)

    @property
    def IGNORE_SPACE(self):
        return self.has(CAPABILITIES.CLIENT_IGNORE_SPACE)

    @property
    def PROTOCOL_41(self):
        return self.has(CAPABILITIES.CLIENT_PROTOCOL_41)

    @property
    def INTERACTIVE(self):
        return self.has(CAPABILITIES.CLIENT_INTERACTIVE)

    @property
    def SSL(self):
        return self.has(CAPABILITIES.CLIENT_SSL)

    @property
    def IGNORE_SIGPIPE(self):
        return self.has(CAPABILITIES.CLIENT_IGNORE_SIGPIPE)

    @property
    def TRANSACTIONS(self):
        return self.has(CAPABILITIES.CLIENT_TRANSACTIONS)

    @property
    def RESERVED(self):
        return self.has(CAPABILITIES.CLIENT_RESERVED)

    @property
    def RESERVED2(self):
        return self.has(CAPABILITIES.CLIENT_RESERVED2)

    @property
    def MULTI_STATEMENTS(self):
        return self.has(CAPABILITIES.CLIENT_MULTI_STATEMENTS)

    @property
    def MULTI_RESULTS(self):
        return self.has(CAPABILITIES.CLIENT_MULTI_RESULTS)

    @property
    def PS_MULTI_RESULTS(self):
        return self.has(CAPABILITIES.CLIENT_PS_MULTI_RESULTS)

    @property
    def PLUGIN_AUTH(self):
        return self.has(CAPABILITIES.CLIENT_PLUGIN_AUTH)

    @property
    def CONNECT_ATTRS(self):
        return self.has(CAPABILITIES.CLIENT_CONNECT_ATTRS)

    @property
    def PLUGIN_AUTH_LENENC_CLIENT_DATA(self):
        return self.has(CAPABILITIES.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)

    @property
    def CAN_HANDLE_EXPIRED_PASSWORDS(self):
        return self.has(CAPABILITIES.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS)

    @property
    def SESSION_TRACK(self):
        return self.has(CAPABILITIES.CLIENT_SESSION_TRACK)

    @property
    def DEPRECATE_EOF(self):
        return self.has(CAPABILITIES.CLIENT_DEPRECATE_EOF)

    @property
    def SSL_VERIFY_SERVER_CERT(self):
        return self.has(CAPABILITIES.CLIENT_SSL_VERIFY_SERVER_CERT)

    @property
    def REMEMBER_OPTIONS(self):
        return self.has(CAPABILITIES.CLIENT_REMEMBER_OPTIONS)

    @property
    def SECURE_CONNECTION(self):
        return self.has(CAPABILITIES.CLIENT_SECURE_CONNECTION)
