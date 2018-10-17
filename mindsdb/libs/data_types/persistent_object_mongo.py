from mindsdb.libs.data_types.object_dict import ObjectDict
from pymongo import MongoClient
import mindsdb.config as CONFIG
from bson.objectid import ObjectId
import logging

class PersistentObjectMongo(ObjectDict):

    _entity_name = 'generic'
    _pkey = []

    def __init__(self):

        self._mongo = MongoClient(CONFIG.MONGO_SERVER_HOST)
        self._collection =  self._mongo.mindsdb[self._entity_name]
        self.setup()


    def setup(self):

        pass


    def insert(self):

        dict = self.getAsDict()
        dict["_id"] = str(ObjectId())
        self._collection.insert(dict)


    def getPkey(self):

        return {key:self.__dict__[key] for key in self._pkey}


    def update(self):

        vals = {key:self.__getattribute__(key) for key in self.getAsDict() if self.__getattribute__(key) != None}
        self._collection.update_one(
            self.getPkey(),
            {'$set': vals}
        )

    def push(self, vals):

        self._collection.update_one(
            self.getPkey(),
            {'$push': vals}
        )


    def delete(self):
        orig_pkey = self.getPkey()
        pkey = {key:orig_pkey[key] for key in orig_pkey if orig_pkey[key] != None}
        self._collection.delete_many(pkey)


    def find_one(self, p_key_data):
        resp = self._collection.find_one(p_key_data)
        class_object = self.__class__()
        if resp is None:
            return  None

        for var_name in resp:
            if hasattr(class_object, var_name):
                setattr(class_object,var_name, resp[var_name])

        return class_object


    def find(self, conditions, order_by= None, limit = None):
        resp = self._collection.find(conditions)
        if order_by is not None:
            resp = resp.sort(order_by)
        if limit is not None:
            if hasattr(resp, 'limit'):
                resp = resp.limit(limit)
            else:
                logging.warning('This driver supports no limit on query')

        ret = []

        if resp is None:
            return  []

        for item in resp:
            class_object = self.__class__()
            for var_name in item:
                if hasattr(class_object, var_name):
                    setattr(class_object,var_name, item[var_name])

            ret.append(class_object)

        return ret

