import sys
import unittest

from vedis import Vedis

from walrus import *


class VedisList(List):
    def extend(self, value):
        return self.database.lmpush(self.key, value)

    def pop(self):
        return self.database.lpop(self.key)


class WalrusVedis(Vedis, Database):
    def __init__(self, filename=':mem:'):
        self._filename = filename
        Vedis.__init__(self, filename)

    def __repr__(self):
        if self._filename in (':memory:', ':mem:'):
            db_file = 'in-memory database'
        else:
            db_file = self._filename
        return '<WalrusVedis: %s>' % db_file

    def execute_command(self, *args, **options):
        raise ValueError('"%s" is not supported by Vedis.' % args[0])

    def parse_response(self, *args, **kwargs):
        raise RuntimeError('Error, parse_response should not be called.')

    def command(self, command_name):
        return self.register(command_name)

    # Compatibility with method names from redis-py.
    def getset(self, key, value):
        return self.get_set(key, value)

    def incrby(self, name, amount=1):
        return self.incr_by(name, amount)

    def decrby(self, name, amount=1):
        return self.decr_by(name, amount)

    # Compatibility with method signatures.
    def mset(self, **data):
        return super(WalrusVedis, self).mset(data)

    def mget(self, *keys):
        return super(WalrusVedis, self).mget(list(keys))

    def __getitem__(self, key):
        try:
            return super(WalrusVedis, self).__getitem__(key)
        except KeyError:
            pass

    def sadd(self, key, *items):
        return super(WalrusVedis, self).smadd(key, list(items))

    # Override the container types since Vedis provides its own using the
    # same method-names as Walrus, and we want the Walrus containers.
    def Hash(self, key):
        return Hash(self, key)

    def Set(self, key):
        return Set(self, key)

    def List(self, key):
        return VedisList(self, key)

    def not_supported(name):
        def decorator(self, *args, **kwargs):
            raise ValueError('%s is not supported by Vedis.' % name)
        return decorator

    ZSet = not_supported('ZSet')
    Array = not_supported('Array')
    HyperLogLog = not_supported('HyperLogLog')
    pipeline = not_supported('pipeline')
    lock = not_supported('lock')
    pubsub = not_supported('pubsub')


class TestWalrusVedis(unittest.TestCase):
    def setUp(self):
        self.db = WalrusVedis()

    def test_basic(self):
        self.db['foo'] = 'bar'
        self.assertEqual(self.db['foo'], b'bar')
        self.assertTrue('foo' in self.db)
        self.assertFalse('xx' in self.db)
        self.assertIsNone(self.db['xx'])

        self.db.mset(k1='v1', k2='v2', k3='v3')
        results = self.db.mget('k1', 'k2', 'k3', 'kx')
        self.assertEqual(list(results), [b'v1', b'v2', b'v3', None])

        self.db.append('foo', 'baz')
        self.assertEqual(self.db.get('foo'), b'barbaz')

        self.db.incr_by('counter', 1)
        self.assertEqual(self.db.incr_by('counter', 5), 6)
        self.assertEqual(self.db.decr_by('counter', 2), 4)

        self.assertEqual(self.db.strlen('foo'), 6)
        self.assertEqual(self.db.getset('foo', 'nug'), b'barbaz')
        self.assertEqual(self.db['foo'], b'nug')

        self.assertFalse(self.db.setnx('foo', 'xxx'))
        self.assertTrue(self.db.setnx('bar', 'yyy'))
        self.assertEqual(self.db['bar'], b'yyy')

        del self.db['foo']
        self.assertFalse('foo' in self.db)

    def test_hash(self):
        h = self.db.Hash('hash_obj')
        h['k1'] = 'v1'
        h.update({'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(h.as_dict(), {
            b'k1': b'v1',
            b'k2': b'v2',
            b'k3': b'v3'})

        self.assertEqual(h['k2'], b'v2')
        self.assertIsNone(h['kx'])
        self.assertTrue('k2' in h)
        self.assertEqual(len(h), 3)

        del h['k2']
        del h['kxx']
        self.assertEqual(sorted(h.keys()), [b'k1', b'k3'])
        self.assertEqual(sorted(h.values()), [b'v1', b'v3'])

    def test_list(self):
        l = self.db.List('list_obj')
        l.prepend('charlie')
        l.extend(['mickey', 'huey', 'zaizee'])
        self.assertEqual(l[0], b'charlie')
        self.assertEqual(l[-1], b'zaizee')
        self.assertEqual(len(l), 4)
        self.assertEqual(l.pop(), b'charlie')

    def test_set(self):
        s = self.db.Set('set_obj')
        s.add('charlie')
        s.add('charlie', 'huey', 'mickey')
        self.assertEqual(len(s), 3)
        self.assertTrue('huey' in s)
        self.assertFalse('xx' in s)
        del s['huey']
        self.assertFalse('huey' in s)
        self.assertEqual(s.members(), set([b'charlie', b'mickey']))

        s1 = self.db.Set('s1')
        s2 = self.db.Set('s2')
        s1.add(*map(str, range(5)))
        s2.add(*map(str, range(3, 7)))
        self.assertEqual(s1 - s2, set([b'0', b'1', b'2']))
        self.assertEqual(s2 - s1, set([b'5', b'6']))
        self.assertEqual(s1 & s2, set([b'3', b'4']))

    def test_unsupported(self):
        def assertUnsupported(cmd, *args):
            method = getattr(self.db, cmd)
            self.assertRaises(ValueError, method, *args)

        # Just check a handful of methods.
        assertUnsupported('zadd', 'zs', {'foo': 1})
        assertUnsupported('ZSet', 'zs')
        assertUnsupported('rpush', 'l_obj', 'val')
        assertUnsupported('rpop', 'l_obj')
        assertUnsupported('ltrim', 'l_obj', 0, 1)
        assertUnsupported('lrem', 'l_obj', 3, 1)

    def test_custom_commands(self):
        @self.db.command('KTITLE')
        def _ktitle_impl(context, key):
            value = context[key]
            if value:
                context[key] = value.title()
                return True
            return False

        self.db['n1'] = 'charlie'
        self.db['n2'] = 'huey'
        self.assertTrue(_ktitle_impl('n1'))
        self.assertEqual(self.db['n1'], b'Charlie')

        self.assertTrue(self.db.execute('KTITLE n2'))
        self.assertEqual(self.db['n2'], b'Huey')

        self.assertFalse(self.db.execute('KTITLE nx'))
        self.assertIsNone(self.db['nx'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
