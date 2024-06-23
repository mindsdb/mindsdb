import datetime as dt
import json
from bson import ObjectId


class MongoJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return f'ISODate({obj.isoformat()})'
        if isinstance(obj, ObjectId):
            return f'ObjectId({str(obj)})'
        return super(MongoJSONEncoder, self).default(obj)


class MongoQuery:

    def __init__(self, collection, pipline=None):
        self.collection = collection
        self.pipeline = []

        if pipline is None:
            pipline = []
        for step in pipline:
            self.add_step(step)

    def add_step(self, step):
        # step = {
        #     'method': 'sort',
        #     'args': [{c: 3}]
        # }

        if 'method' not in step \
                or 'args' not in step \
                or not isinstance(step['args'], list):
            raise AttributeError(f'Wrong step {step}')

        self.pipeline.append(step)

    def to_string(self):
        return self.__str__()

    def __getattr__(self, item):
        # return callback to save step of pipeline
        def fnc(*args):
            self.pipeline.append({
                'method': item,
                'args': args
            })
        return fnc

    def __str__(self):
        """
        converts call to string

        {
            'collection': 'fish',
            'call': [   // call is sequence of methods
                {
                    'method': 'find',
                    'args': [{a:1}, {b:2}]
                },
                {
                    'method': 'sort',
                    'args': [{c:3}]
                },
            ]
        }

        to:

         "db_test.fish.find({a:1}, {b:2}).sort({c:3})"
        """

        call_str = f'db.{self.collection}'
        for step in self.pipeline:
            args_str = []
            for arg in step['args']:
                args_str.append(MongoJSONEncoder().encode(arg))
            call_str += f'.{step["method"]}({",".join(args_str)})'
        return call_str

    def __repr__(self):
        return f'MongoQuery({self.collection}, {str(self.pipeline)})'
