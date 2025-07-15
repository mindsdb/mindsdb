import time
import types
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


def execute_in_threads(func, tasks, thread_count=3, queue_size_k=1.5):
    """
    Should be used as generator.
    Can accept input tasks as generator and keep queue size the same to not overflow the RAM

    :param func: callable, function to execute in threads
    :param tasks: generator or iterable, list of input for function
    :param thread_count: number of threads
    :param queue_size_k: how a queue for workers is bigger than count of threads
    :return: yield results
    """
    executor = ContextThreadPoolExecutor(max_workers=thread_count)

    queue_size = int(thread_count * queue_size_k)

    if not isinstance(tasks, types.GeneratorType):
        tasks = iter(tasks)

    futures = None
    while futures is None or len(futures) > 0:
        if futures is None:
            futures = []

        # add new portion
        for i in range(queue_size):
            try:
                args = next(tasks)
                futures.append(executor.submit(func, args))
            except StopIteration:
                break

        # save results
        for task in futures:
            if task.done():
                yield task.result()

        # remove completed tasks
        futures[:] = [t for t in futures if not t.done()]

        time.sleep(0.1)
    executor.shutdown(wait=False)
