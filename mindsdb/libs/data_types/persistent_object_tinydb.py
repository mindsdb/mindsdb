from mindsdb.libs.data_types.object_dict import ObjectDict
from mindsdb.libs.data_types.persistent_object_mongo import PersistentObjectMongo
from tinymongo import TinyMongoClient
from mindsdb.libs.helpers.logging import logging

import mindsdb.config as CONFIG
import shutil


class PersistentObjectTinydb(PersistentObjectMongo):

    _entity_name = 'generic'
    _pkey = []

    def __init__(self):

        self._mongo = TinyMongoClient(CONFIG.LOCALSTORE_PATH)
        try:
            self._collection =  self._mongo.mindsdb[self._entity_name]
        except:
            logging.error('No collection will be found, db corrupted, truncating it')
            shutil.rmtree(CONFIG.LOCALSTORE_PATH)
            raise ValueError('MindsDB local document store corruped. No other way to put this, trained model data will be lost')

        self.setup()




    def push(self, vals):
        """
        Tinymongo does not support push, so here we have it
        :param vals:
        :return:
        """
        obj_dict = self._collection.find_one(self.getPkey())

        new_vals = {}
        for key in vals:
            if key not in obj_dict:
                obj_dict[key] = []
            if type(vals[key]) == type([]):
                new_vals[key] = obj_dict[key] + vals[key]
            else:
                new_vals[key] = obj_dict[key] + [vals[key]]


        self._collection.update_one(
            self.getPkey(),
            {'$set': vals}
        )


