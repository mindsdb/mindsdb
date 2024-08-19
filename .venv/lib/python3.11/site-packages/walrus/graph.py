# Hexastore.
import itertools
import json

from walrus.utils import decode


class _VariableGenerator(object):
    def __getattr__(self, name):
        return Variable(name)

    def __call__(self, name):
        return Variable(name)


class Graph(object):
    """
    Simple hexastore built using Redis ZSets. The basic idea is that we have
    a collection of relationships of the form subject-predicate-object. For
    example:

    * charlie -- friends -- huey
    * charlie -- lives -- Kansas
    * huey -- lives -- Kansas

    We might wish to ask questions of our data-store like "which of charlie's
    friends live in Kansas?" To do this we will store every permutation of
    the S-P-O triples, then we can do efficient queries using the parts of
    the relationship we know:

    * query the "object" portion of the "charlie -- friends" subject
      and predicate.
    * for each object returned, turn it into the subject of a second query
      whose predicate is "lives" and whose object is "Kansas"

    So we would return the subjects that satisfy the following expression::

        ("charlie -- friends") -- lives -- Kansas.

    To accomplish this in Python we could write:

    .. code-block:: python

        db = Database()
        graph = db.graph('people')

        # Store my friends.
        graph.store_many(
            ('charlie', 'friends', 'huey'),
            ('charlie', 'friends', 'zaizee'),
            ('charlie', 'friends', 'nuggie'))

        # Store where people live.
        graph.store_many(
            ('huey', 'lives', 'Kansas'),
            ('zaizee', 'lives', 'Missouri'),
            ('nuggie', 'lives', 'Kansas'),
            ('mickey', 'lives', 'Kansas'))

        # Perform our search. We will use a variable (X) to indicate the
        # value we're interested in.
        X = graph.v.X  # Create a variable placeholder.

        # In the first clause we indicate we are searching for my friends.
        # In the second clause, we only want those friends who also live in
        # Kansas.
        results = graph.search(
            {'s': 'charlie', 'p': 'friends', 'o': X},
            {'s': X, 'p': 'lives', 'o': 'Kansas'})
        print results

        # Prints: {'X': {'huey', 'nuggie'}}

    See: http://redis.io/topics/indexes#representing-and-querying-graphs-using-an-hexastore
    """
    def __init__(self, walrus, namespace):
        self.walrus = walrus
        self.namespace = namespace
        self.v = _VariableGenerator()
        self._z = self.walrus.ZSet(self.namespace)

    def store(self, s, p, o):
        """
        Store a subject-predicate-object triple in the database.
        """
        with self.walrus.atomic():
            for key in self.keys_for_values(s, p, o):
                self._z[key] = 0

    def store_many(self, items):
        """
        Store multiple subject-predicate-object triples in the database.

        :param items: A list of (subj, pred, obj) 3-tuples.
        """
        with self.walrus.atomic():
            for item in items:
                self.store(*item)

    def delete(self, s, p, o):
        """Remove the given subj-pred-obj triple from the database."""
        with self.walrus.atomic():
            for key in self.keys_for_values(s, p, o):
                del self._z[key]

    def keys_for_values(self, s, p, o):
        parts = [
            ('spo', s, p, o),
            ('pos', p, o, s),
            ('osp', o, s, p)]
        for part in parts:
            yield '::'.join(part)

    def keys_for_query(self, s=None, p=None, o=None):
        parts = []
        key = lambda parts: '::'.join(parts)

        if s and p and o:
            parts.extend(('spo', s, p, o))
            return key(parts), None
        elif s and p:
            parts.extend(('spo', s, p))
        elif s and o:
            parts.extend(('osp', s, o))
        elif p and o:
            parts.extend(('pos', p, o))
        elif s:
            parts.extend(('spo', s))
        elif p:
            parts.extend(('pos', p))
        elif o:
            parts.extend(('osp', o))
        else:
            parts.extend(('spo',))
        return key(parts + ['']), key(parts + ['\xff'])

    def query(self, s=None, p=None, o=None):
        """
        Return all triples that satisfy the given expression. You may specify
        all or none of the fields (s, p, and o). For instance, if I wanted
        to query for all the people who live in Kansas, I might write:

        .. code-block:: python

            for triple in graph.query(p='lives', o='Kansas'):
                print triple['s'], 'lives in Kansas!'
        """
        start, end = self.keys_for_query(s, p, o)
        if end is None:
            if start in self._z:
                yield {'s': s, 'p': p, 'o': o}
        else:
            for key in self._z.range_by_lex('[' + start, '[' + end):
                keys, p1, p2, p3 = decode(key).split('::')
                yield dict(zip(keys, (p1, p2, p3)))

    def v(self, name):
        """
        Create a named variable, used to construct multi-clause queries with
        the :py:meth:`Graph.search` method.
        """
        return Variable(name)

    def search(self, *conditions):
        """
        Given a set of conditions, return all values that satisfy the
        conditions for a given set of variables.

        For example, suppose I wanted to find all of my friends who live in
        Kansas:

        .. code-block:: python

            X = graph.v.X
            results = graph.search(
                {'s': 'charlie', 'p': 'friends', 'o': X},
                {'s': X, 'p': 'lives', 'o': 'Kansas'})

        The return value consists of a dictionary keyed by variable, whose
        values are ``set`` objects containing the values that satisfy the
        query clauses, e.g.:

        .. code-block:: python

            print results

            # Result has one key, for our "X" variable. The value is the set
            # of my friends that live in Kansas.
            # {'X': {'huey', 'nuggie'}}

            # We can assume the following triples exist:
            # ('charlie', 'friends', 'huey')
            # ('charlie', 'friends', 'nuggie')
            # ('huey', 'lives', 'Kansas')
            # ('nuggie', 'lives', 'Kansas')
        """
        results = {}

        for condition in conditions:
            if isinstance(condition, tuple):
                query = dict(zip('spo', condition))
            else:
                query = condition.copy()
            materialized = {}
            targets = []

            for part in ('s', 'p', 'o'):
                if isinstance(query[part], Variable):
                    variable = query.pop(part)
                    materialized[part] = set()
                    targets.append((variable, part))

            # Potentially rather than popping all the variables, we could use
            # the result values from a previous condition and do O(results)
            # loops looking for a single variable.
            for result in self.query(**query):
                ok = True
                for var, part in targets:
                    if var in results and result[part] not in results[var]:
                        ok = False
                        break

                if ok:
                    for var, part in targets:
                        materialized[part].add(result[part])

            for var, part in targets:
                if var in results:
                    results[var] &= materialized[part]
                else:
                    results[var] = materialized[part]

        return dict((var.name, vals) for (var, vals) in results.items())


class Variable(object):
    __slots__ = ['name']

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Variable: %s>' % (self.name)
