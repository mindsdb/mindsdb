import unittest

import pytest
from mindsdb_sql_parser import parse_sql

try:
    from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_render import MongodbRender
    from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_parser import MongodbParser
    MONGODB_HANDLER_AVAILABLE = True
except ImportError:
    MONGODB_HANDLER_AVAILABLE = False

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_mongodb_handler.py


@pytest.mark.skipif(not MONGODB_HANDLER_AVAILABLE, reason="mongodb_handler not installed (community handler)")
class TestMongoDBConverters(unittest.TestCase):
    def test_ast_to_mongo(self):
        sql = """
            select *
             from tbl1
            where x!=1 and c=2 and d>4  or e is not null
            order by d, e desc
        """

        # sql to ast
        query = parse_sql(sql)
        mql = MongodbRender().to_mongo_query(query)

        expected_mql = """
          db.tbl1.aggregate([
            {"$match": {"$or": [
                {"$and": [{"$and": [
                     {"x": {"$ne": 1}},
                     {"c": 2}]},
                     {"d": {"$gt": 4}}]},
                {"e": {"$ne": null}}]}},
            {"$sort": {"d": -1, "e": -1}}
          ])
        """.replace("\n", "")

        # test ast to mongo
        assert mql.to_string().replace(" ", "") == expected_mql.replace(" ", "")

        # test parsing: mql to string and then string to mql
        mql_str = mql.to_string()
        print(mql_str)
        assert MongodbParser().from_string(mql_str).to_string() == mql_str

        sql = """
           select distinct a.b, a.c  from tbl1
           where x=1
           limit 2
           offset 3
        """

        query = parse_sql(sql)
        mql = MongodbRender().to_mongo_query(query)

        expected_mql = """
            db.tbl1.aggregate([
               {"$match": {"x": 1}},
               {"$group": {
                   "_id": {"b": "$b", "c": "$c"},
                   "b": {"$first": "$b"},
                   "c": {"$first": "$c"}
               }},
               {"$project": {"_id": 0, "b": "$b", "c": "$c"}},
               {"$skip": 3},
               {"$limit": 2}
            ])
        """.replace("\n", "")

        # test ast to mongo
        assert mql.to_string().replace(" ", "") == expected_mql.replace(" ", "")

        sql = """
            select a as name, sum(b) as total, count(c) as cnt
            from tbl1
            where x>=5
            group by a
            order by cnt desc
        """

        query = parse_sql(sql)
        mql = MongodbRender().to_mongo_query(query)

        expected_mql = """
            db.tbl1.aggregate([
                {"$match": {"x": {"$gte": 5}}},
                {"$group": {"_id": {"a": "$a"}, "total": {"$sum": "$b"}, "cnt": {"$sum": {"$cond": [{"$ne": ["$c", null]}, 1, 0]}}}},
                {"$project": {"_id": 0, "name": "$a", "total": "$total", "cnt": "$cnt"}},
                {"$sort": {"cnt": -1}}
            ])
        """.replace("\n", "")

        assert mql.to_string().replace(" ", "") == expected_mql.replace(" ", "")

        # TODO use in queries:  multiline, objectid, isodate
        # covered in tests/unit/handlers/test_mongodb.py

    def test_mongo_parser(self):
        mql = """
           db.TransactionFact.find(
              {'_id': '0', "a": "1", b: 1}
           )
        """

        expected_mql = 'db.TransactionFact.find({"_id": "0", "a": "1", "b": 1})'

        assert MongodbParser().from_string(mql).to_string() == expected_mql


@pytest.mark.skipif(not MONGODB_HANDLER_AVAILABLE, reason="mongodb_handler not installed (community handler)")
class TestMongoDBHandler(unittest.TestCase):
    def test_mongo_handler(self):
        # TODO how to test mongo handler
        #   test mysql query
        #   test mongo query
        pass
