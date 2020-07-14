from mindsdb.api.mysql.mysql_proxy.datahub.information_schema import InformationSchema
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.mindsdb_datanode import MindsDBDataNode


def init_datahub(config):
    all_ds = config['api']['mysql'].get('datasources', [])

    datahub = InformationSchema()

    datahub.add({
        'mindsdb': MindsDBDataNode(config)
    })

    return datahub
