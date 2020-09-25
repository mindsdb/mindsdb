from mindsdb.api.mongo.classes import Responder
import mindsdb.api.mongo.functions as helpers

# GET OpMSG=OrderedDict([('_recvChunkStart', 'config.system.sessions'), ('uuid', UUID('7d78b9de-c962-4ec8-afb1-cfa4d36028f9')), ('lsid', OrderedDict([('id', UUID('5ed016fe-d46d-476e-9b97-441f7d986642')), ('uid', b"\xbb\x89\xd3\x17_\xb0\x98\x1c\x86\x82l'u\x90\x82\xa3q`G\x1f`PKF\x12AQ\x86\xc60\xabp")])), ('txnNumber', 26), ('sessionId', 'replmain_shard0000_5f6da0792cd48e5402913997'), ('from', 'replmain/127.0.0.1:27001'), ('fromShardName', 'replmain'), ('toShardName', 'shard0000'), ('min', OrderedDict([('_id', MinKey())])), ('max', OrderedDict([('_id', OrderedDict([('id', UUID('00400000-0000-0000-0000-000000000000'))]))])), ('shardKeyPattern', OrderedDict([('_id', 1)])), ('writeConcern', OrderedDict()), ('$clusterTime', OrderedDict([('clusterTime', Timestamp(1601020025, 3)), ('signature', OrderedDict([('hash', b'3}\x0e\xe8\x81\xa30{\xc5"F\xfe\xbe\x96\x14\xcdB\x00:\xec'), ('keyId', 6876326860909707272)]))])), ('$configServerState', OrderedDict([('opTime', OrderedDict([('ts', Timestamp(1601020024, 3)), ('t', 1)]))])), ('$db', 'admin')])


class Responce(Responder):
    when = {'_recvChunkStart': helpers.is_true}

    result = response = {
        "ok": 0
    }


responder = Responce()
