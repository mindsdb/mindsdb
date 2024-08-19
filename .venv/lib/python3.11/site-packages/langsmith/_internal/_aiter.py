"""Adapted.

Original source:
https://github.com/maxfischer2781/asyncstdlib/blob/master/asyncstdlib/itertools.py
MIT License
"""

import asyncio
import inspect
from collections import deque
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

T = TypeVar("T")

_no_default = object()


# https://github.com/python/cpython/blob/main/Lib/test/test_asyncgen.py#L54
# before 3.10, the builtin anext() was not available
def py_anext(
    iterator: AsyncIterator[T], default: Union[T, Any] = _no_default
) -> Awaitable[Union[T, None, Any]]:
    """Pure-Python implementation of anext() for testing purposes.

    Closely matches the builtin anext() C implementation.
    Can be used to compare the built-in implementation of the inner
    coroutines machinery to C-implementation of __anext__() and send()
    or throw() on the returned generator.
    """
    try:
        __anext__ = cast(
            Callable[[AsyncIterator[T]], Awaitable[T]], type(iterator).__anext__
        )
    except AttributeError:
        raise TypeError(f"{iterator!r} is not an async iterator")

    if default is _no_default:
        return __anext__(iterator)

    async def anext_impl() -> Union[T, Any]:
        try:
            # The C code is way more low-level than this, as it implements
            # all methods of the iterator protocol. In this implementation
            # we're relying on higher-level coroutine concepts, but that's
            # exactly what we want -- crosstest pure-Python high-level
            # implementation and low-level C anext() iterators.
            return await __anext__(iterator)
        except StopAsyncIteration:
            return default

    return anext_impl()


class NoLock:
    """Dummy lock that provides the proper interface but no protection."""

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        return False


async def tee_peer(
    iterator: AsyncIterator[T],
    # the buffer specific to this peer
    buffer: Deque[T],
    # the buffers of all peers, including our own
    peers: List[Deque[T]],
    lock: AsyncContextManager[Any],
) -> AsyncGenerator[T, None]:
    """Iterate over :py:func:`~.tee`."""
    try:
        while True:
            if not buffer:
                async with lock:
                    # Another peer produced an item while we were waiting for the lock.
                    # Proceed with the next loop iteration to yield the item.
                    if buffer:
                        continue
                    try:
                        item = await iterator.__anext__()
                    except StopAsyncIteration:
                        break
                    else:
                        # Append to all buffers, including our own. We'll fetch our
                        # item from the buffer again, instead of yielding it directly.
                        # This ensures the proper item ordering if any of our peers
                        # are fetching items concurrently. They may have buffered their
                        # item already.
                        for peer_buffer in peers:
                            peer_buffer.append(item)
            yield buffer.popleft()
    finally:
        async with lock:
            # this peer is done – remove its buffer
            for idx, peer_buffer in enumerate(peers):  # pragma: no branch
                if peer_buffer is buffer:
                    peers.pop(idx)
                    break
            # if we are the last peer, try and close the iterator
            if not peers and hasattr(iterator, "aclose"):
                await iterator.aclose()


class Tee(Generic[T]):
    """Create ``n`` separate asynchronous iterators over ``iterable``.

    This splits a single ``iterable`` into multiple iterators, each providing
    the same items in the same order.
    All child iterators may advance separately but pare the same items
    from ``iterable`` -- when the most advanced iterator retrieves an item,
    it is buffered until the least advanced iterator has yielded it as well.
    A ``tee`` works lazily and can handle an infinite ``iterable``, provided
    that all iterators advance.

    .. code-block:: python3

        async def derivative(sensor_data):
            previous, current = a.tee(sensor_data, n=2)
            await a.anext(previous)  # advance one iterator
            return a.map(operator.sub, previous, current)

    Unlike :py:func:`itertools.tee`, :py:func:`~.tee` returns a custom type instead
    of a :py:class:`tuple`. Like a tuple, it can be indexed, iterated and unpacked
    to get the child iterators. In addition, its :py:meth:`~.tee.aclose` method
    immediately closes all children, and it can be used in an ``async with`` context
    for the same effect.

    If ``iterable`` is an iterator and read elsewhere, ``tee`` will *not*
    provide these items. Also, ``tee`` must internally buffer each item until the
    last iterator has yielded it; if the most and least advanced iterator differ
    by most data, using a :py:class:`list` is more efficient (but not lazy).

    If the underlying iterable is concurrency safe (``anext`` may be awaited
    concurrently) the resulting iterators are concurrency safe as well. Otherwise,
    the iterators are safe if there is only ever one single "most advanced" iterator.
    To enforce sequential use of ``anext``, provide a ``lock``
    - e.g. an :py:class:`asyncio.Lock` instance in an :py:mod:`asyncio` application -
    and access is automatically synchronised.
    """

    def __init__(
        self,
        iterable: AsyncIterator[T],
        n: int = 2,
        *,
        lock: Optional[AsyncContextManager[Any]] = None,
    ):
        self._iterator = iterable.__aiter__()  # before 3.10 aiter() doesn't exist
        self._buffers: List[Deque[T]] = [deque() for _ in range(n)]
        self._children = tuple(
            tee_peer(
                iterator=self._iterator,
                buffer=buffer,
                peers=self._buffers,
                lock=lock if lock is not None else NoLock(),
            )
            for buffer in self._buffers
        )

    def __len__(self) -> int:
        return len(self._children)

    @overload
    def __getitem__(self, item: int) -> AsyncIterator[T]: ...

    @overload
    def __getitem__(self, item: slice) -> Tuple[AsyncIterator[T], ...]: ...

    def __getitem__(
        self, item: Union[int, slice]
    ) -> Union[AsyncIterator[T], Tuple[AsyncIterator[T], ...]]:
        return self._children[item]

    def __iter__(self) -> Iterator[AsyncIterator[T]]:
        yield from self._children

    async def __aenter__(self) -> "Tee[T]":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        await self.aclose()
        return False

    async def aclose(self) -> None:
        for child in self._children:
            await child.aclose()


atee = Tee


async def async_zip(*async_iterables):
    """Async version of zip."""
    # Before Python 3.10, aiter() was not available
    iterators = [iterable.__aiter__() for iterable in async_iterables]
    while True:
        try:
            items = await asyncio.gather(
                *(py_anext(iterator) for iterator in iterators)
            )
            yield tuple(items)
        except StopAsyncIteration:
            break


def ensure_async_iterator(
    iterable: Union[Iterable, AsyncIterable],
) -> AsyncIterator:
    if hasattr(iterable, "__anext__"):
        return cast(AsyncIterator, iterable)
    elif hasattr(iterable, "__aiter__"):
        return cast(AsyncIterator, iterable.__aiter__())
    else:

        class AsyncIteratorWrapper:
            def __init__(self, iterable: Iterable):
                self._iterator = iter(iterable)

            async def __anext__(self):
                try:
                    return next(self._iterator)
                except StopIteration:
                    raise StopAsyncIteration

            def __aiter__(self):
                return self

        return AsyncIteratorWrapper(iterable)


def aiter_with_concurrency(
    n: Optional[int], generator: AsyncIterator[Coroutine[None, None, T]]
) -> AsyncGenerator[T, None]:
    """Process async generator with max parallelism.

    Args:
        n: The number of tasks to run concurrently.
        generator: The async generator to process.

    Yields:
        The processed items yielded by the async generator.
    """
    if n == 0:

        async def consume():
            async for item in generator:
                yield await item

        return consume()
    semaphore = asyncio.Semaphore(n) if n is not None else NoLock()

    async def process_item(item):
        async with semaphore:
            return await item

    async def process_generator():
        tasks = []
        async for item in generator:
            task = asyncio.create_task(process_item(item))
            tasks.append(task)
            if n is not None and len(tasks) >= n:
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                tasks = list(pending)
                for task in done:
                    yield task.result()

        for task in asyncio.as_completed(tasks):
            yield await task

    return process_generator()


def accepts_context(callable: Callable[..., Any]) -> bool:
    """Check if a callable accepts a context argument."""
    try:
        return inspect.signature(callable).parameters.get("context") is not None
    except ValueError:
        return False
