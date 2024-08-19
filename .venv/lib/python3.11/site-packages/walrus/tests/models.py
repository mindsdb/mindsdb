import datetime

from walrus import *
from walrus.query import OP_AND
from walrus.query import OP_OR
from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class BaseModel(Model):
    __database__ = db
    __namespace__ = 'test'


class User(BaseModel):
    username = TextField(primary_key=True)


class Note(BaseModel):
    user = TextField(index=True)
    text = TextField()
    timestamp = DateTimeField(default=datetime.datetime.now, index=True)
    tags = JSONField()


class Message(BaseModel):
    content = TextField(fts=True)
    status = IntegerField(default=1, index=True)


class FTSOptions(BaseModel):
    content = TextField(fts=True, stemmer=True)
    metaphone = TextField(fts=True, stemmer=True, metaphone=True)


class Stat(BaseModel):
    key = AutoIncrementField()
    stat_type = ByteField(index=True)
    value = IntegerField(index=True)


class DefaultOption(BaseModel):
    default_empty = JSONField()
    txt = TextField(default='')
    num = IntegerField(default=0)


class TestModels(WalrusTestCase):
    def create_objects(self):
        for i in range(3):
            u = User.create(username='u%s' % (i + 1))
            for j in range(3):
                Note.create(
                    user=u.username,
                    text='n%s-%s' % (i + 1, j + 1),
                    tags=['t%s' % (k + 1) for k in range(j)])

    def test_textfield_whitespace(self):
        h = User.create(username='huey cat')
        z = User.create(username='zaizee cat')
        h_db = User.load('huey cat')
        self.assertEqual(h_db.username, 'huey cat')
        z_db = User.load('zaizee cat')
        self.assertEqual(z_db.username, 'zaizee cat')

        query = User.query(User.username == 'huey cat')
        self.assertEqual([u.username for u in query], ['huey cat'])
        self.assertRaises(KeyError, User.load, 'mickey dog')

    def test_store_none(self):
        class Simple(BaseModel):
            text = TextField()
            number = IntegerField()
            normalized = FloatField()

        s = Simple.create(text=None, number=None, normalized=None)
        s_db = Simple.load(s._id)
        self.assertEqual(s_db.text, '')
        self.assertEqual(s_db.number, 0)
        self.assertEqual(s_db.normalized, 0.)

    def test_create(self):
        self.create_objects()
        self.assertEqual(
            sorted(user.username for user in User.all()),
            ['u1', 'u2', 'u3'])

        notes = Note.query(Note.user == 'u1')
        self.assertEqual(
            sorted(note.text for note in notes),
            ['n1-1', 'n1-2', 'n1-3'])

        notes = sorted(
            Note.query(Note.user == 'u2'),
            key = lambda note: note._id)
        note = notes[2]
        self.assertEqual(note.tags, ['t1', 't2'])

    def test_exceptions(self):
        self.assertRaises(KeyError, User.load, 'charlie')
        User.create(username='charlie')
        user = User.load('charlie')
        self.assertEqual(user.username, 'charlie')

    def test_query(self):
        self.create_objects()
        notes = Note.query(Note.user == 'u2')
        self.assertEqual(
            sorted(note.text for note in notes),
            ['n2-1', 'n2-2', 'n2-3'])

        user = User.get(User.username == 'u3')
        self.assertEqual(user._data, {'username': 'u3'})

        self.assertRaises(ValueError, User.get, User.username == 'ux')

    def test_query_with_update(self):
        stat = Stat.create(stat_type='s1', value=1)
        vq = list(Stat.query(Stat.value == 1))
        self.assertEqual(len(vq), 1)
        stat_db = vq[0]
        self.assertEqual(stat_db.stat_type, b's1')
        self.assertEqual(stat_db.value, 1)

        stat.value = 2
        stat.save()

        def assertCount(expr, count):
            self.assertEqual(len(list(Stat.query(expr))), count)

        assertCount(Stat.value == 1, 0)
        assertCount(Stat.value == 2, 1)
        assertCount(Stat.stat_type == 's1', 1)

        stat.stat_type = 's2'
        stat.save()

        assertCount(Stat.value == 1, 0)
        assertCount(Stat.value == 2, 1)
        assertCount(Stat.stat_type == 's1', 0)
        assertCount(Stat.stat_type == 's2', 1)

    def test_sorting(self):
        self.create_objects()
        all_notes = [
            'n1-1', 'n1-2', 'n1-3', 'n2-1', 'n2-2', 'n2-3', 'n3-1', 'n3-2',
            'n3-3']

        notes = Note.query(order_by=Note.text)
        self.assertEqual([note.text for note in notes], all_notes)

        notes = Note.query(order_by=Note.text.desc())
        self.assertEqual(
            [note.text for note in notes],
            all_notes[::-1])

        notes = Note.query(Note.user == 'u2', Note.text)
        self.assertEqual(
            [note.text for note in notes],
            ['n2-1', 'n2-2', 'n2-3'])

        notes = Note.query(Note.user == 'u2', Note.text.desc())
        self.assertEqual(
            [note.text for note in notes],
            ['n2-3', 'n2-2', 'n2-1'])

    def test_complex_query(self):
        usernames = ['charlie', 'huey', 'mickey', 'zaizee']
        for username in usernames:
            User.create(username=username)

        def assertUsers(expr, expected):
            users = User.query(expr)
            self.assertEqual(
                sorted(user.username for user in users),
                sorted(expected))

        assertUsers(User.username == 'charlie', ['charlie'])
        assertUsers(User.username != 'huey', ['charlie', 'mickey', 'zaizee'])
        assertUsers(
            ((User.username == 'charlie') | (User.username == 'mickey')),
            ['charlie', 'mickey'])
        assertUsers(
            (User.username == 'charlie') | (User.username != 'mickey'),
            ['charlie', 'huey', 'zaizee'])
        expr = (
            ((User.username != 'huey') & (User.username != 'zaizee')) |
            (User.username == 'charlie'))
        assertUsers(expr, ['charlie', 'mickey'])

    def test_scalar_query(self):
        data = [
            ('t1', 1),
            ('t1', 2),
            ('t1', 3),
            ('t2', 10),
            ('t2', 11),
            ('t2', 12),
            ('t3', 0),
        ]
        for stat_type, value in data:
            Stat.create(stat_type=stat_type, value=value)

        stat_objects = sorted(
            (stat for stat in Stat.all()),
            key=lambda stat: stat.key)
        self.assertEqual([stat._data for stat in stat_objects], [
            {'key': 1, 'stat_type': b't1', 'value': 1},
            {'key': 2, 'stat_type': b't1', 'value': 2},
            {'key': 3, 'stat_type': b't1', 'value': 3},
            {'key': 4, 'stat_type': b't2', 'value': 10},
            {'key': 5, 'stat_type': b't2', 'value': 11},
            {'key': 6, 'stat_type': b't2', 'value': 12},
            {'key': 7, 'stat_type': b't3', 'value': 0},
        ])

        def assertStats(expr, expected):
            stats = Stat.query(expr)
            self.assertEqual(
                sorted(stat.key for stat in stats),
                sorted(expected))

        assertStats(Stat.value <= 3, [1, 2, 3, 7])
        assertStats(Stat.value >= 10, [4, 5, 6])
        assertStats(Stat.value < 3, [1, 2, 7])
        assertStats(Stat.value > 10, [5, 6])

        assertStats(Stat.value == 3, [3])
        assertStats(Stat.value >= 13, [])
        assertStats(
            (Stat.value <= 2) | (Stat.key >= 7),
            [1, 2, 7])
        assertStats(
            ((Stat.value <= 2) & (Stat.key >= 7)) | (Stat.value >= 11),
            [5, 6, 7])
        assertStats(
            ((Stat.value <= 2) | (Stat.key >= 7)) & (Stat.stat_type == 't1'),
            [1, 2])

        assertStats(Stat.value.between(2, 11), [2, 3, 4, 5])
        assertStats(Stat.value.between(4, 12), [4, 5, 6])

    def test_full_text_search(self):
        phrases = [
            ('A faith is a necessity to a man. Woe to him who believes in '
             'nothing.'),
            ('All who call on God in true faith, earnestly from the heart, '
             'will certainly be heard, and will receive what they have asked '
             'and desired.'),
            ('Be faithful in small things because it is in them that your '
             'strength lies.'),
            ('Faith consists in believing when it is beyond the power of '
             'reason to believe.'),
            ('Faith has to do with things that are not seen and hope with '
             'things that are not at hand.')]

        for idx, message in enumerate(phrases):
            Message.create(content=message, status=1 + (idx % 2))

        def assertMatches(query, indexes):
            results = [message.content for message in query]
            self.assertEqual(results, [phrases[i] for i in indexes])

        def assertSearch(search, indexes):
            assertMatches(
                Message.query(Message.content.match(search)),
                indexes)

        assertSearch('faith', [1, 4, 3, 2, 0])
        assertSearch('faith man', [0])
        assertSearch('things', [2, 4])
        assertSearch('blah', [])

        query = Message.query(
            Message.content.match('faith') & (Message.status == 1))
        assertMatches(query, [4, 2, 0])

    def test_full_text_combined(self):
        phrases = [
            'little bunny foo foo',  # 0, s=1
            'a little green owl',  # 1, s=2
            'the owl was named foo',  # 2, s=1
            'he had a nicotine patch on his wing',  # 3, s=2
            'he was trying to quit smoking',  # 4, s=1
            'the owl was little and green and sweet',  # 5, s=2
            'he dropped presents on my porch',  # 6, s=1
        ]
        index_to_phrase = {}
        for idx, message in enumerate(phrases):
            msg = Message.create(content=message, status=1 + (idx % 2))
            index_to_phrase[idx] = message

        def assertSearch(search, indexes):
            self.assertEqual(
                sorted(message.content for message in query),
                sorted(index_to_phrase[idx] for idx in indexes))

        query = Message.query(Message.content.match('little owl'))
        assertSearch(query, [1, 5, 2])  # "little" is ignored (stop word).

        query = Message.query(
            Message.content.match('little owl') &
            Message.content.match('foo'))
        assertSearch(query, [2])

        query = Message.query(
            (Message.content.match('owl') & (Message.status == 1)) |
            (Message.content.match('foo') & (Message.status == 2)))
        assertSearch(query, [2])

        query = Message.query(
            (Message.content.match('green') & (Message.status == 2)) |
            (Message.status == 1))
        assertSearch(query, [0, 2, 4, 6, 1, 5])

        query = Message.query(
            ((Message.status == 2) & Message.content.match('green')) |
            (Message.status == 1))
        assertSearch(query, [0, 2, 4, 6, 1, 5])

    def test_full_text_options(self):
        phrases = [
            'building web applications with python and flask',
            'modern web development with python',
            'unit testing with python',
            'writing better tests for your application',
            'applications for the web',
        ]

        for phrase in phrases:
            FTSOptions.create(content=phrase, metaphone=phrase)

        def assertMatches(search, indexes, use_metaphone=False):
            if use_metaphone:
                field = FTSOptions.metaphone
            else:
                field = FTSOptions.content
            query = FTSOptions.query(field.match(search))
            results = [message.content for message in query]
            self.assertEqual(results, [phrases[i] for i in indexes])

        assertMatches('web application', [0, 4])
        assertMatches('web application', [0, 4], True)

        assertMatches('python', [0, 1, 2])
        assertMatches('python', [0, 1, 2], True)

        assertMatches('testing', [3, 2])
        assertMatches('testing', [3, 2], True)

        # Test behavior of the metaphone algorithm.
        assertMatches('python flasck', [0], True)
        assertMatches('pithon devellepment', [], False)
        assertMatches('pithon devellepment', [1], True)
        assertMatches('younit tessts', [2], True)

    def test_fts_query_parser(self):
        messages = [
            'foo green',
            'bar green',
            'baz blue',
            'nug blue',
            'nize yellow',
            'huey greener',
            'mickey greens',
            'zaizee',
        ]
        for message in messages:
            Message.create(content=message)

        def assertMatches(query, expected, default_conjunction=OP_AND):
            expression = Message.content.search(query, default_conjunction)
            messages = Message.query(expression, order_by=Message.content)
            results = [msg.content for msg in messages]
            self.assertEqual(results, expected)

        assertMatches('foo', ['foo green'])
        assertMatches('foo OR baz', ['baz blue', 'foo green'])
        assertMatches('green OR blue', [
            'bar green',
            'baz blue',
            'foo green',
            'mickey greens',
            'nug blue',
        ])
        assertMatches('green AND (bar OR mickey OR nize)', [
            'bar green',
            'mickey greens',
        ])
        assertMatches('zaizee OR (blue AND nug) OR (green AND bar)', [
            'bar green',
            'nug blue',
            'zaizee',
        ])
        assertMatches('(blue AND (baz OR (nug OR huey OR mickey))', [
            'baz blue',
            'nug blue',
        ])
        assertMatches(
            '(blue OR foo) AND (green OR (huey OR (baz AND mickey)))',
            ['foo green'])

        assertMatches('(green AND nug) OR (blue AND bar)', [])
        assertMatches('nuglet', [])
        assertMatches('foobar', [])
        assertMatches('', sorted(messages))

    def test_load(self):
        User.create(username='charlie')
        u = User.load('charlie')
        self.assertEqual(u._data, {'username': 'charlie'})

    def test_save_delete(self):
        charlie = User.create(username='charlie')
        huey = User.create(username='huey')
        note = Note.create(user='huey', text='n1')
        note.text = 'n1-edited'
        note.save()

        self.assertEqual(
            sorted(user.username for user in User.all()),
            ['charlie', 'huey'])

        notes = Note.all()
        self.assertEqual([note.text for note in notes], ['n1-edited'])

        charlie.delete()
        self.assertEqual([user.username for user in User.all()], ['huey'])

    def test_delete_indexes(self):
        self.assertEqual(set(db.keys()), set())

        Message.create(content='charlie message', status=1)
        Message.create(content='huey message', status=2)
        keys = set(db.keys())

        charlie = Message.load(1)
        charlie.delete()
        huey_keys = set(db.keys())

        diff = keys - huey_keys

        def make_key(*args):
            return Message._query.make_key(*args).encode('utf-8')

        self.assertEqual(diff, set([
            make_key('_id', 'absolute', 1),
            make_key('content', 'absolute', 'charlie message'),
            make_key('content', 'fts', 'charli'),
            make_key('id', 1),
            make_key('status', 'absolute', 1),
        ]))

        # Ensure we cannot query for Charlie, but that we can query for Huey.
        expressions = [
            (Message.status == 1),
            (Message.status != 2),
            (Message._id == 1),
            (Message._id != 2),
            (Message.content == 'charlie message'),
            (Message.content != 'huey message'),
            (Message.content.match('charlie')),
        ]
        for expression in expressions:
            self.assertRaises(ValueError, Message.get, expression)

        expressions = [
            (Message.status == 2),
            (Message.status > 1),
            (Message._id == 2),
            (Message._id != 1),
            (Message.content == 'huey message'),
            (Message.content != 'charlie'),
            (Message.content.match('huey')),
            (Message.content.match('message')),
        ]
        for expression in expressions:
            obj = Message.get(expression)
            self.assertEqual(obj._data, {
                '_id': 2,
                'content': 'huey message',
                'status': 2,
            })

        after_filter_keys = set(db.keys())
        symm_diff = huey_keys ^ after_filter_keys
        self.assertTrue(all(key.startswith(b'temp') for key in symm_diff))

        huey = Message.load(2)
        huey.delete()

        final_keys = set(key for key in db.keys()
                         if not key.startswith(b'temp'))
        self.assertEqual(final_keys, set([make_key('_id', '_sequence')]))

    def test_get_regression(self):
        Message.create(content='huey', status=1)
        Message.create(content='charlie', status=2)

        def assertMessage(msg, data):
            self.assertEqual(msg._data, data)

        huey = {'_id': 1, 'content': 'huey', 'status': 1}
        charlie = {'_id': 2, 'content': 'charlie', 'status': 2}
        assertMessage(Message.load(1), huey)
        assertMessage(Message.load(2), charlie)

        for i in range(3):
            assertMessage(Message.get(Message._id == 1), huey)
            assertMessage(Message.get(Message._id == 2), charlie)

            assertMessage(Message.get(Message.status == 1), huey)
            assertMessage(Message.get(Message.status == 2), charlie)
            assertMessage(Message.get(Message.status != 1), charlie)
            assertMessage(Message.get(Message.status != 2), huey)

            messages = list(Message.query(Message.status == 1))
            self.assertEqual(len(messages), 1)
            assertMessage(messages[0], huey)
            messages = list(Message.query(Message.status != 1))
            self.assertEqual(len(messages), 1)
            assertMessage(messages[0], charlie)

    def test_index_separator(self):
        class CustomSeparator(BaseModel):
            index_separator = '$'
            name = TextField(primary_key=True)
            data = IntegerField(index=True)

        CustomSeparator.create(name='huey.zai', data=3)
        CustomSeparator.create(name='michael.nuggie', data=5)

        keys = sorted(db.keys())
        self.assertEqual(keys, [
            # namespace | model : $-delimited indexed data
            b'test|customseparator:all',
            b'test|customseparator:data$absolute$3',
            b'test|customseparator:data$absolute$5',
            b'test|customseparator:data$continuous',
            b'test|customseparator:id$huey.zai',
            b'test|customseparator:id$michael.nuggie',
            b'test|customseparator:name$absolute$huey.zai',
            b'test|customseparator:name$absolute$michael.nuggie'])

        huey = CustomSeparator.get(CustomSeparator.data < 5)
        self.assertEqual(huey.name, 'huey.zai')

        mickey = CustomSeparator.load('michael.nuggie')
        self.assertEqual(mickey.data, 5)

    def test_incr(self):
        for i in range(3):
            Stat.create(stat_type='test', value=i)

        s1 = Stat.get(Stat.value == 1)
        res = s1.incr(Stat.value, 5)
        self.assertEqual(res, 6)
        self.assertEqual(s1.value, 6)

        self.assertRaises(ValueError, Stat.get, Stat.value == 1)
        s6 = Stat.get(Stat.value == 6)
        self.assertEqual(s1.key, s6.key)

    def test_count(self):
        self.assertEqual(User.count(), 0)

        for username in ['charlie', 'leslie', 'connor']:
            User.create(username=username)

        self.assertEqual(User.count(), 3)

    def test_query_delete(self):
        for i in range(5):
            u = User.create(username='u%s' % (i + 1))

        User.query_delete((User.username == 'u1') | (User.username == 'u4'))
        usernames = [user.username for user in User.all()]
        self.assertEqual(sorted(usernames), ['u2', 'u3', 'u5'])

        User.query_delete()
        self.assertEqual([user for user in User.all()], [])

    def test_container_field_persistence(self):
        class HashModel(BaseModel):
            data = HashField()
            name = TextField()

        hm1 = HashModel.create(name='hm1')
        hm1.data.update(k1='v1', k2='v2')

        hm2 = HashModel.create(name='hm2')
        hm2.data.update(k3='v3', k4='v4')

        hm1.name = 'hm1-e'
        hm1.save()

        hm1_db = HashModel.load(hm1._id)
        self.assertEqual(hm1_db.name, 'hm1-e')
        self.assertEqual(hm1.data.as_dict(), {b'k1': b'v1', b'k2': b'v2'})

    def test_delete_container_fields(self):
        class HashModel(BaseModel):
            data = HashField()
            name = TextField()

        hm1 = HashModel.create(name='hm1')
        hm1.data.update(k1='v1', k2='v2')

        hm2 = HashModel.create(name='hm2')
        hm2.data.update(k3='v3', k4='v4')

        hm1.delete()
        self.assertEqual(hm1.data.as_dict(), {})
        self.assertEqual(hm2.data.as_dict(), {b'k3': b'v3', b'k4': b'v4'})

    def test_default_is_an_empty_dict(self):
        instance = DefaultOption()
        self.assertTrue(instance.default_empty is None)
        self.assertEqual(instance.num, 0)
        self.assertEqual(instance.txt, '')

    def test_json_storage(self):
        class APIResponse(BaseModel):
            data = JSONField()

        ar = APIResponse(data={'k1': 'v1', 'k2': 'v2'})
        ar.save()

        ar_db = APIResponse.load(ar._id)
        self.assertEqual(ar_db.data, {'k1': 'v1', 'k2': 'v2'})

    def test_pickled_storage(self):
        class PythonData(BaseModel):
            data = PickledField()

        pd = PythonData(data={'k1': ['v1', None, 'v3']})
        pd.save()

        pd_db = PythonData.load(pd._id)
        self.assertEqual(pd_db.data, {'k1': ['v1', None, 'v3']})

        pd2 = PythonData.create(data=None)
        pd2_db = PythonData.load(pd2._id)
        self.assertTrue(pd2_db.data is None)

    def test_boolean_field(self):
        class Account(BaseModel):
            name = TextField(primary_key=True)
            active = BooleanField()
            admin = BooleanField(default=False)

        charlie = Account(name='charlie', active=True, admin=True)
        huey = Account(name='huey', active=False)
        charlie.save()
        huey.save()

        charlie_db = Account.get(Account.name == 'charlie')
        self.assertTrue(charlie_db.active)
        self.assertTrue(charlie_db.admin)

        huey_db = Account.get(Account.name == 'huey')
        self.assertFalse(huey_db.active)
        self.assertFalse(huey_db.admin)

        huey_db.active = True
        huey_db.admin = True
        huey_db.save()

        huey_db2 = Account.get(Account.name == 'huey')
        self.assertTrue(huey_db2.active)
        self.assertTrue(huey_db2.admin)

    def test_query_boolean(self):
        class BT(BaseModel):
            key = TextField(primary_key=True)
            flag = BooleanField(default=False, index=True)

        for i in range(4):
            BT.create(key='k%s' % i, flag=True if i % 2 else False)

        query = BT.query(BT.flag == True)
        self.assertEqual(sorted([bt.key for bt in query]), ['k1', 'k3'])
        query = BT.query(BT.flag == False)
        self.assertEqual(sorted([bt.key for bt in query]), ['k0', 'k2'])

    def test_uuid(self):
        class Beacon(BaseModel):
            name = TextField(primary_key=True)
            data = UUIDField()

        b1 = Beacon.create(name='alpha', data=uuid.uuid4())
        b2 = Beacon.create(name='bravo', data=uuid.uuid4())
        b3 = Beacon.create(name='charlie')
        b3_db = Beacon.load('charlie')
        b2_db = Beacon.load('bravo')
        b1_db = Beacon.load('alpha')
        self.assertEqual(b1.data, b1_db.data)
        self.assertEqual(b2.data, b2_db.data)
        self.assertTrue(b3.data is None)

    def _test_date_field(self, field_class, dt_func):
        class Event(BaseModel):
            timestamp = field_class(index=True)
            value = TextField()

        events = [
            Event.create(timestamp=dt_func(i), value='e%s' % i)
            for i in range(1, 11)]

        e_db = Event.get(Event._id == events[-1]._id)
        self.assertEqual(e_db.timestamp, dt_func(10))
        self.assertEqual(e_db.value, 'e10')

        events = Event.query(
            (Event.timestamp >= dt_func(3)) &
            (Event.timestamp < dt_func(7)), Event.timestamp)
        ts2value = [(e.timestamp, e.value) for e in events]
        self.assertEqual(ts2value, [
            (dt_func(3), 'e3'),
            (dt_func(4), 'e4'),
            (dt_func(5), 'e5'),
            (dt_func(6), 'e6')])

        e = Event.create(value='ex')
        e_db = Event.load(e._id)
        self.assertTrue(e_db.timestamp is None)
        self.assertEqual(e_db.value, 'ex')

    def test_datetime_field(self):
        dt = lambda day: datetime.datetime(2018, 1, day, 3, 13, 37)
        self._test_date_field(DateTimeField, dt)

    def test_date_field(self):
        dt = lambda day: datetime.date(2018, 1, day)
        self._test_date_field(DateField, dt)


if __name__ == '__main__':
    import unittest; unittest.main()
