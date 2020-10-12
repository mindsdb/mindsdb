from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers

# GET OpMSG=OrderedDict([('_addShard', 1), ('shardIdentity', OrderedDict([('shardName', 'shard0000'), ('clusterId', ObjectId('5f6c7d3e4bf2f9c3da4dc92f')), ('configsvrConnectionString', 'replconf/127.0.0.1:27000')])), ('$db', 'admin')])
# real full response for v4.4 = {
#     "ok": 1,
#     "$gleStats": {
#         "lastOpTime": {
#             "ts": 6876008835761307649,
#             "t": 1
#         }
#         "electionId": ObjectId(7f:ff:ff:ff:00:00:00:00:00:00:00:01)
#     },
#     "lastCommittedOpTime": 6876008835761307649,
#     "$configServerState": {
#         "opTime": {
#             "ts": 6876008831466340353,
#             "t": 1
#         }
#     },
#     "$clusterTime": {
#         "clusterTime": 6876008835761307650,
#         "signature": {
#             "hash": 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00,
#             "keyId": 0
#         }
#     },
#     "operationTime": 6876008835761307649
# }


class Responce(Responder):
    when = {'_addShard': helpers.is_true}

    result = response = {
        "ok": 1
    }


responder = Responce()
