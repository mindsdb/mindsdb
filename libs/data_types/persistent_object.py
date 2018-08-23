from libs.data_types.object_dict import ObjectDict
from pymongo import MongoClient
import config as CONFIG
from bson.objectid import ObjectId

class PersistentObject(ObjectDict):

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
        self._collection.insert(self.getAsDict())


    def getPkey(self):

        return {key:self.__dict__[key] for key in self._pkey}


    def update(self):

        vals = {key:self.__getattribute__(key) for key in self.getAsDict() if self.__getattribute__(key) != None}
        self._collection.update_one(
            self.getPkey(),
            {'$set': vals}
        )


    def delete(self):
        orig_pkey = self.getPkey()
        pkey = {key:orig_pkey[key] for key in orig_pkey if orig_pkey[key] != None}
        self._collection.delete_many(pkey)