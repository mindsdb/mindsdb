from mindsdb.api.mysql.mysql_proxy.datahub.information_schema import InformationSchema
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.mindsdb_datanode import MindsDBDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datasource_datanode import DataSourceDataNode
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.integration_datanode import IntegrationDataNode


def init_datahub(config):
    # TODO remove 'datasources' from config
    # all_ds = config['api']['mysql'].get('datasources', [])

    datahub = InformationSchema()

    datahub.add({
        'mindsdb': MindsDBDataNode(config),
        'datasource': DataSourceDataNode(config),
        'test_mariadb': IntegrationDataNode(config, 'test_mariadb')
    })

    for key in config['integrations']:
        datahub.add({
            key: IntegrationDataNode(config, key)
        })

    return datahub
