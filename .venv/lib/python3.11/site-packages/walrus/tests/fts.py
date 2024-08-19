#coding:utf-8
from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


class TestSearchIndex(WalrusTestCase):
    def test_search_index(self):
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

        index = db.Index('test-index')

        for idx, message in enumerate(phrases):
            index.add('doc-%s' % idx, message)

        def assertDocs(query, indexes):
            results = [doc['content'] for doc in index.search(query)]
            self.assertEqual(results, [phrases[i] for i in indexes])

        assertDocs('faith', [0, 2, 3, 4, 1])
        assertDocs('faith man', [0])
        assertDocs('things', [4, 2])
        assertDocs('blah', [])

    def test_add_remove_update(self):
        data = [
            ('huey cat', {'type': 'cat', 'color': 'white'}),
            ('zaizee cat cat', {'type': 'cat', 'color': 'gray'}),
            ('mickey dog', {'type': 'dog', 'color': 'black'}),
            ('beanie cat', {'type': 'cat', 'color': 'gray'}),
        ]

        idx = db.Index('test-index')
        for i, (content, metadata) in enumerate(data):
            idx.add(str(i), content, **metadata)

        huey, = idx.search('huey')
        self.assertEqual(huey, {
            'content': 'huey cat',
            'type': 'cat',
            'color': 'white'})

        self.assertEqual([d['content'] for d in idx.search('cat')],
                         ['zaizee cat cat', 'huey cat', 'beanie cat'])

        idx.remove('3')  # Poor beanie :(
        zaizee, huey = idx.search('cat')
        self.assertEqual(zaizee['content'], 'zaizee cat cat')
        self.assertEqual(huey['content'], 'huey cat')

        self.assertRaises(KeyError, idx.remove, '3')

        idx.update('1', 'zaizee cat', {'type': 'kitten'})
        idx.replace('0', 'huey baby cat', {'type': 'kitten'})

        zaizee, huey = idx.search('cat')
        self.assertEqual(zaizee['content'], 'zaizee cat')
        self.assertEqual(zaizee['type'], 'kitten')
        self.assertEqual(zaizee['color'], 'gray')

        self.assertEqual(huey['content'], 'huey baby cat')
        self.assertEqual(huey['type'], 'kitten')
        self.assertTrue('color' not in huey)

        zaizee, huey = idx.search_items('cat')
        self.assertEqual(zaizee[0], '1')
        self.assertEqual(zaizee[1]['content'], 'zaizee cat')
        self.assertEqual(huey[0], '0')
        self.assertEqual(huey[1]['content'], 'huey baby cat')

    def test_search_phonetic(self):
        data = (
            ('pf', 'python and flask'),
            ('lcp', 'learning cython programming'),
            ('lwd', 'learning web development with flask'),
            ('pwd', 'python web development'))
        data_dict = dict(data)
        idx = db.Index('test-index', metaphone=True)
        for key, content in data:
            idx.add(key, content)

        def assertResults(query, keys):
            result = idx.search(query)
            self.assertEqual([doc['content'] for doc in result],
                             [data_dict[key] for key in keys])

        assertResults('flasck', ['pf', 'lwd'])
        assertResults('pythonn', ['pf', 'pwd'])
        assertResults('sithon', ['lcp'])
        assertResults('webb development', ['pwd', 'lwd'])

        assertResults('sithon OR (flasck AND pythonn)', ['pf', 'lcp'])
        assertResults('garbage', [])

    def test_search_parser(self):
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
        index = db.Index('testing')

        for idx, message in enumerate(messages):
            index.add(str(idx), message)

        def assertMatches(query, expected):
            results = [doc['content'] for doc in index.search(query)]
            self.assertEqual(results, expected)

        assertMatches('foo', ['foo green'])
        assertMatches('foo OR baz', ['foo green', 'baz blue'])
        assertMatches('green OR blue', [
            'foo green',
            'bar green',
            'baz blue',
            'nug blue',
            'mickey greens',
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
        assertMatches('foo"bar green', ['foo green'])

        results = [doc['content'] for doc in index.search('')]
        self.assertEqual(sorted(results), sorted(messages))

    def test_unicode_handling(self):
        index = db.Index('testing', stemmer=False)
        index.add('1', u'сколько лет этому безумному моржу', {'val': 'age'})
        index.add('2', u'во сколько морж ложится спать', val='sleep')
        index.add('3', u'Вы знаете какие-нибудь хорошие истории с моржами',
                  val='stories')
        self.assertEqual([r['val'] for r in index.search(u'морж')], ['sleep'])
