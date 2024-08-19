import threading

from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestLock(WalrusTestCase):
    def test_lock(self):
        lock_a = db.lock('lock-a')
        lock_b = db.lock('lock-b')
        self.assertTrue(lock_a.acquire())
        self.assertTrue(lock_b.acquire())

        lock_a2 = db.lock('lock-a')
        self.assertFalse(lock_a2.acquire(block=False))
        self.assertFalse(lock_a2.release())
        self.assertNotEqual(lock_a._lock_id, lock_a2._lock_id)

        self.assertFalse(lock_a.acquire(block=False))
        self.assertFalse(lock_b.acquire(block=False))

        t_waiting = threading.Event()
        t_acquired = threading.Event()
        t_acknowledged = threading.Event()

        def wait_for_lock():
            lock_a = db.lock('lock-a')
            t_waiting.set()
            lock_a.acquire()
            t_acquired.set()
            t_acknowledged.wait()
            lock_a.release()

        waiter_t = threading.Thread(target=wait_for_lock)
        waiter_t.start()
        t_waiting.wait()  # Wait until the thread is up and running.

        lock_a.release()
        t_acquired.wait()
        self.assertFalse(lock_a.acquire(block=False))
        t_acknowledged.set()
        waiter_t.join()
        self.assertTrue(lock_a.acquire(block=False))
        lock_a.release()

    def test_lock_ctx_mgr(self):
        lock_a = db.lock('lock-a')
        lock_a2 = db.lock('lock-a')
        with lock_a:
            self.assertFalse(lock_a2.acquire(block=False))
        self.assertTrue(lock_a2.acquire(block=False))

    def test_lock_decorator(self):
        lock = db.lock('lock-a')

        @lock
        def locked():
            lock2 = db.lock('lock-a')
            self.assertFalse(lock2.acquire(block=False))

        locked()

        @lock
        def raise_exception():
            raise ValueError()

        self.assertRaises(ValueError, raise_exception)

        # In the event of an exception, the lock will still be released.
        self.assertTrue(lock.acquire(block=False))

    def test_lock_cleanup(self):
        self.assertEqual(len(db), 0)
        lock = db.lock('lock-a')
        self.assertTrue(lock.acquire())
        self.assertTrue(lock.release())
        self.assertEqual(len(db), 1)  # We have the lock event key.
        self.assertEqual(db.lpop(lock.event), b'1')
