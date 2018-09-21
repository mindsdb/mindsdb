from mindsdb.libs.data_types.object_dict import ObjectDict
from mindsdb.libs.data_types.persistent_object_mongo import PersistentObjectMongo
from mindsdb.libs.data_types.persistent_object_tinydb import PersistentObjectTinydb

from pymongo import MongoClient
import mindsdb.config as CONFIG
from bson.objectid import ObjectId

if CONFIG.STORE_INFO_IN_MONGODB:
    PersistentObject = PersistentObjectMongo
else:
    PersistentObject = PersistentObjectTinydb

