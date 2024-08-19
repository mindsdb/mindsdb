from walrus.containers import *
from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestWalrus(WalrusTestCase):
    def test_get_key(self):
        h = db.Hash('h1')
        h['hk1'] = 'v1'

        l = db.List('l1')
        l.append('i1')

        s = db.Set('s1')
        s.add('k1')

        zs = db.ZSet('z1')
        zs.add({'i1': 1., 'i2': 2.})

        h_db = db.get_key('h1')
        self.assertTrue(isinstance(h_db, Hash))
        self.assertEqual(h_db['hk1'], b'v1')

        l_db = db.get_key('l1')
        self.assertTrue(isinstance(l_db, List))
        self.assertEqual(l_db[0], b'i1')

        s_db = db.get_key('s1')
        self.assertTrue(isinstance(s_db, Set))
        self.assertEqual(s_db.members(), set((b'k1',)))

        z_db = db.get_key('z1')
        self.assertTrue(isinstance(z_db, ZSet))
        self.assertEqual(z_db.score('i1'), 1.)

    def test_atomic(self):
        def assertDepth(depth):
            self.assertEqual(len(db._transaction_local.pipes), depth)

        assertDepth(0)
        with db.atomic() as p1:
            assertDepth(1)
            with db.atomic() as p2:
                assertDepth(2)
                with db.atomic() as p3:
                    assertDepth(3)
                    p3.pipe.set('k3', 'v3')

                assertDepth(2)
                self.assertEqual(db['k3'], b'v3')

                p2.pipe.set('k2', 'v2')

            assertDepth(1)
            self.assertEqual(db['k3'], b'v3')
            self.assertEqual(db['k2'], b'v2')
            p1.pipe.set('k1', 'v1')

        assertDepth(0)
        self.assertEqual(db['k1'], b'v1')
        self.assertEqual(db['k2'], b'v2')
        self.assertEqual(db['k3'], b'v3')

    def test_atomic_exception(self):
        def do_atomic(k, v, exc=False):
            with db.atomic() as a:
                a.pipe.set(k, v)
                if exc:
                    raise TypeError('foo')

        do_atomic('k', 'v')
        self.assertEqual(db['k'], b'v')

        self.assertRaises(TypeError, do_atomic, 'k2', 'v2', True)
        self.assertRaises(KeyError, lambda: db['k2'])
        self.assertEqual(db._transaction_local.pipe, None)

        # Try nested failure.
        with db.atomic() as outer:
            outer.pipe.set('k2', 'v2')
            self.assertRaises(TypeError, do_atomic, 'k3', 'v3', True)

            # Only this will be set.
            outer.pipe.set('k4', 'v4')

        self.assertTrue(db._transaction_local.pipe is None)
        self.assertEqual(db['k2'], b'v2')
        self.assertRaises(KeyError, lambda: db['k3'])
        self.assertEqual(db['k4'], b'v4')

    def test_clear_transaction(self):
        with db.atomic() as a1:
            a1.pipe.set('k1', 'v1')
            with db.atomic() as a2:
                a2.pipe.set('k2', 'v2')
                a2.clear()

        self.assertEqual(db['k1'], b'v1')
        self.assertRaises(KeyError, lambda: db['k2'])

        with db.atomic() as a1:
            a1.pipe.set('k3', 'v3')
            with db.atomic() as a2:
                self.assertRaises(KeyError, lambda: db['k3'])

                a2.pipe.set('k4', 'v4')
                a2.clear()

            a1.pipe.set('k5', 'v5')

        self.assertEqual(db['k3'], b'v3')
        self.assertRaises(KeyError, lambda: db['k4'])
        self.assertEqual(db['k5'], b'v5')

        self.assertTrue(db._transaction_local.pipe is None)

    def test_cas(self):
        db['k1'] = 'v1'
        self.assertTrue(db.cas('k1', 'v1', 'v1-x'))
        self.assertFalse(db.cas('k1', 'v1-z', 'v1-y'))

        self.assertEqual(db['k1'], b'v1-x')
        self.assertTrue(db.cas('k1', 'v1-', 'v2'))
        self.assertFalse(db.cas('k1', 'v1', 'v3'))
        self.assertEqual(db['k1'], b'v2')
