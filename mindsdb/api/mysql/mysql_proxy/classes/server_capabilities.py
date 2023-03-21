from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import DEFAULT_CAPABILITIES


class ServerCapabilities():
    def __init__(self, capabilities):
        self._capabilities = capabilities

    def has(self, cap):
        return cap & self._capabilities > 0

    def set(self, cap, value=True):
        if value:
            self._capabilities = self._capabilities | cap
        else:
            self._capabilities = self._capabilities & (~cap)

    @property
    def value(self):
        return self._capabilities


server_capabilities = ServerCapabilities(DEFAULT_CAPABILITIES)
