import os
import unittest
from distutils.version import StrictVersion

from walrus import Database


HOST = os.environ.get('WALRUS_REDIS_HOST') or '127.0.0.1'
PORT = os.environ.get('WALRUS_REDIS_PORT') or 6379

db = Database(host=HOST, port=PORT, db=15)


REDIS_VERSION = None


def requires_version(min_version):
    def decorator(fn):
        global REDIS_VERSION
        if REDIS_VERSION is None:
            REDIS_VERSION = db.info()['redis_version']
        too_old = StrictVersion(REDIS_VERSION) < StrictVersion(min_version)
        return unittest.skipIf(too_old,
                               'redis too old, requires %s' % min_version)(fn)
    return decorator


def stream_test(fn):
    test_stream = os.environ.get('TEST_STREAM')
    if not test_stream:
        return requires_version('4.9.101')(fn)
    else:
        return unittest.skipIf(not test_stream, 'skipping stream tests')(fn)


def zpop_test(fn):
    test_zpop = os.environ.get('TEST_ZPOP')
    if not test_zpop:
        return requires_version('4.9.101')(fn)
    else:
        return unittest.skipIf(not test_zpop, 'skipping zpop* tests')(fn)


class WalrusTestCase(unittest.TestCase):
    def setUp(self):
        db.flushdb()
        db._transaction_local.pipes = []

    def tearDown(self):
        db.flushdb()
        db._transaction_local.pipes = []

    def assertList(self, values, expected):
        values = list(values)
        self.assertEqual(len(values), len(expected))
        for value, item in zip(values, expected):
            self.assertEqual(value, item)
