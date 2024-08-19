import datetime

from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


cache = db.cache(name='test.cache')


@cache.cached(timeout=60)
def now(seed=None):
    return datetime.datetime.now()


class Clock(object):
    @cache.cached_property()
    def now(self):
        return datetime.datetime.now()


class TestCache(WalrusTestCase):
    def test_cache_apis(self):
        # Nonexistant key returns None.
        self.assertTrue(cache.get('foo') is None)

        # Set key, value and expiration in seconds.
        self.assertEqual(cache.set('foo', 'bar', 60), True)
        self.assertEqual(cache.get('foo'), 'bar')

        self.assertEqual(cache.delete('foo'), 1)
        self.assertTrue(cache.get('foo') is None)
        self.assertEqual(cache.delete('foo'), 0)

    def test_cache_bulk_apis(self):
        self.assertEqual(cache.get_many(['k1', 'k2']), {})

        data = {'k1': 'v1', 'k2': 'v2'}
        self.assertEqual(cache.set_many(data, 60), True)
        self.assertEqual(cache.get_many(['k1', 'kx', 'k2']), data)
        self.assertEqual(cache.delete_many(['k1', 'kx', 'k2']), 2)
        self.assertEqual(cache.get_many(['k1', 'k2']), {})
        self.assertEqual(cache.delete_many(['k1', 'kx', 'k2']), 0)

    def test_cache_decorator(self):
        n0 = now()  # Each should have its own cache-key.
        n1 = now(1)
        n2 = now(2)

        self.assertTrue(n0 != n1 != n2)
        self.assertEqual(now(), n0)
        self.assertEqual(now(1), n1)
        self.assertEqual(now(2), n2)

        now.bust(1)
        self.assertNotEqual(now(1), n1)
        self.assertEqual(now(1), now(1))

    def test_cached_property(self):
        c = Clock()
        n1 = c.now
        n2 = c.now
        self.assertEqual(n1, n2)

        del c.now  # Property deleter busts the cache.
        n3 = c.now
        self.assertTrue(n1 != n3)
        self.assertEqual(c.now, n3)

    def test_cached_return_none(self):
        S = {'count': 0}
        @cache.cached()
        def returns_none(arg):
            S['count'] += 1

        def assertMisses(arg, n):
            returns_none(arg)
            self.assertEqual(S['count'], n)
        for i in range(3):
            assertMisses('foo', 1)
        assertMisses('bar', 2)
        assertMisses('foo', 2)

    def test_cached_async(self):
        @cache.cache_async()
        def double_value(value):
            return value * 2

        res = double_value(3)
        self.assertEqual(res(), 6)
        self.assertEqual(res(), 6)

        self.assertEqual(double_value(3)(), 6)
        self.assertEqual(double_value(4)(), 8)

    def test_flush_empty_cache(self):
        cache.set('foo', 'bar', 10)
        self.assertList(cache.keys(), ['test.cache:foo'])
        cache.flush()
        self.assertList(cache.keys(), [])
