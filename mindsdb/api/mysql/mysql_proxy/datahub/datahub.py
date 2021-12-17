from mindsdb.api.mysql.mysql_proxy.datahub.information_schema import InformationSchema
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.integration_datanode import IntegrationDataNode
# from mindsdb.interfaces.database.integrations import DatasourceController


def init_datahub(model_interface, ai_table, data_store, datasource_interface, company_id=None):
    datahub = InformationSchema(model_interface, ai_table, data_store, datasource_interface)

    integrations = datasource_interface.get_db_integrations().keys()
    for key in integrations:
        datahub.add({
            key: IntegrationDataNode(key, data_store)
        })

    return datahub
