from concurrent.futures import ThreadPoolExecutor
import contextvars


class ContextThreadPoolExecutor(ThreadPoolExecutor):
    '''Handles copying context variables to threads created by ThreadPoolExecutor'''
    def __init__(self, max_workers=None):
        self.context = contextvars.copy_context()
        # ThreadPoolExecutor does not propagate context to threads by default, so we need a custom initializer.
        super().__init__(max_workers=max_workers, initializer=self._set_child_context)

    def _set_child_context(self):
        for var, value in self.context.items():
            var.set(value)
