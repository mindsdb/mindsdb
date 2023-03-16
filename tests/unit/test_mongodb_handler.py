import unittest
from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.mongodb_handler.utils.mongodb_render import MongodbRender
from mindsdb.api.mongo.utilities.mongodb_parser import MongodbParser

# How to run:
#  env PYTHONPATH=./ pytest tests/unit/test_mongodb_handler.py


class TestMongoDBConverters(unittest.TestCase):

    def test_ast_to_mongo(self):

        sql = '''
            select *
             from tbl1
            where x!=1 and c=2 and d>4  or e is not null
            order by d, e desc
        '''

        # sql to ast
        query = parse_sql(sql, 'mindsdb')
        mql = MongodbRender().to_mongo_query(query)

        expected_mql = '''
          db.tbl1.aggregate([
            {"$match": {"$or": [
                {"$and": [{"$and": [
                     {"x": {"$ne": 1}},
                     {"c": 2}]}, 
                     {"d": {"$gt": 4}}]}, 
                {"e": {"$ne": null}}]}}, 
            {"$sort": {"d": -1, "e": -1}}
          ])
        '''.replace('\n', '')

        # test ast to mongo
        assert mql.to_string().replace(' ', '') == expected_mql.replace(' ', '')

        # test parsing: mql to string and then string to mql
        mql_str = mql.to_string()
        print(mql_str)
        assert MongodbParser().from_string(mql_str).to_string() == mql_str


        sql = '''
           select distinct a.b, a.c  from tbl1
           where x=1 
           limit 2
           offset 3
        '''

        query = parse_sql(sql, 'mindsdb')
        mql = MongodbRender().to_mongo_query(query)

        expected_mql = '''
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
        '''.replace('\n', '')

        # test ast to mongo
        assert mql.to_string().replace(' ', '') == expected_mql.replace(' ', '')

        # TODO test group

        # TODO use in queries:  multiline, objectid, isodate


class TestMongoDBHandler(unittest.TestCase):

    def test_mongo_handler(self):

        # TODO how to test mongo handler
        #   test mysql query
        #   test mongo query
        pass

