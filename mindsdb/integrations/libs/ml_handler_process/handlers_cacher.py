import time
from collections import UserDict


class HandlersCache(UserDict):
    def __init__(self, max_size: int = 5) -> None:
        self._max_size = max_size
        super().__init__()

    def __setitem__(self, key, value) -> None:
        if len(self.data) > self._max_size:
            sorted_elements = sorted(
                self.data.items(),
                key=lambda x: x[1]['last_usage_at']
            )
            del self.data[sorted_elements[0][0]]
        self.data[key] = {
            'last_usage_at': time.time(),
            'handler': value
        }

    def __getitem__(self, key: int) -> object:
        el = super().__getitem__(key)
        el['last_usage_at'] = time.time()
        return el['handler']


handlers_cacher = HandlersCache()
