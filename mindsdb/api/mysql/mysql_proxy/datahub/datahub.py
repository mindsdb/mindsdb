from mindsdb.api.mysql.mysql_proxy.datahub.datanodes import InformationSchemaDataNode


def init_datahub(session):
    datahub = InformationSchemaDataNode(session)

    return datahub
