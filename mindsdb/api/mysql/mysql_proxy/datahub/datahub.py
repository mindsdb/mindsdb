from mindsdb.api.mysql.mysql_proxy.datahub.information_schema import InformationSchema
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.mindsdb_datanode import MindsDBDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datasource_datanode import DataSourceDataNode


def init_datahub(config):
    # TODO remove 'datasources' from config
    # all_ds = config['api']['mysql'].get('datasources', [])

    datahub = InformationSchema()

    datahub.add({
        'mindsdb': MindsDBDataNode(config),
        'datasource': DataSourceDataNode(config)
    })

    return datahub
