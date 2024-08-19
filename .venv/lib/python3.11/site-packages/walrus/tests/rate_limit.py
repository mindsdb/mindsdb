import time

from walrus.rate_limit import RateLimitException
from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestRateLimit(WalrusTestCase):
    def setUp(self):
        super(TestRateLimit, self).setUp()
        # Limit to 5 events per second.
        self.rl = self.get_rate_limit('test-rl', 5, 1)

    def get_rate_limit(self, key, limit, per):
        return db.rate_limit(key, limit, per)

    def test_rate_limit(self):
        for i in range(5):
            self.assertFalse(self.rl.limit('k1'))

        for i in range(3):
            self.assertTrue(self.rl.limit('k1'))

        self.assertFalse(self.rl.limit('k2'))

    def test_rate_limit_rollover(self):
        rl = self.get_rate_limit('test-rl2', 3, 100)
        container = db.List('test-rl2:k1')

        now = time.time()
        past = now - 101

        # Simulate two events.
        container.extend([now, now])

        # Third event goes through OK.
        self.assertFalse(rl.limit('k1'))

        # Fourth event is rate-limited.
        self.assertTrue(rl.limit('k1'))

        # There are three timestamps in the container.
        self.assertEqual(len(container), 3)

        # Hand modify the oldest timestamp to appear as if it happened over
        # 100 seconds ago.
        container[-1] = past

        # We can again perform an action.
        self.assertFalse(rl.limit('k1'))

        # We once again have 3 items all within the last 100 seconds, so we
        # are rate-limited.
        self.assertTrue(rl.limit('k1'))

        # There are only 3 items in the container.
        self.assertEqual(len(container), 3)

        # The oldest item is the 2nd we added at the beginning of the test.
        self.assertEqual(float(container[-1]), now)

        # Remove an item and make the 2nd timestamp (oldest) in the past. This
        # gives us 2 actions.
        container.popright()
        container[-1] = past

        self.assertFalse(rl.limit('k1'))
        self.assertFalse(rl.limit('k1'))
        self.assertTrue(rl.limit('k1'))

    def test_decorator(self):
        rl = self.get_rate_limit('test-rl2', 3, 100)
        container = db.List('test-rl2:fake-key')

        def key_fn(*args, **kwargs):
            return 'fake-key'

        @rl.rate_limited(key_function=key_fn)
        def do_test():
            return 'OK'

        now = time.time()
        container.extend([now, now])

        self.assertEqual(do_test(), 'OK')
        self.assertRaises(RateLimitException, do_test)

        container.popright()
        container[-1] = now - 101

        self.assertEqual(do_test(), 'OK')
        self.assertEqual(do_test(), 'OK')
        self.assertRaises(RateLimitException, do_test)


class TestRateLimitLua(TestRateLimit):
    def get_rate_limit(self, key, limit, per):
        return db.rate_limit_lua(key, limit, per)
