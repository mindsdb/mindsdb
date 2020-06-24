from mindsdb.api.mysql.mysql_proxy.datahub.information_schema import InformationSchema
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.mindsdb_datanode import MindsDBDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.mongo_datanode import MongoDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.csv_datanode import CSVDataNode


def init_datahub(config):
    all_ds = config['api']['mysql'].get('datasources', [])

    datahub = InformationSchema()

    datahub.add({
        'mindsdb': MindsDBDataNode(config)
    })

    csv_ds = [x for x in all_ds if x['type'].lower() == 'csv']
    for ds in csv_ds:
        datahub.add({
            ds['name']: CSVDataNode(ds['files'])
        })

    mongo_ds = [x for x in all_ds if x['type'].lower() == 'mongo']
    for ds in mongo_ds:
        datahub.add({
            ds['name']: MongoDataNode(ds['host'], ds['port'], ds['database'])
        })
    return datahub
