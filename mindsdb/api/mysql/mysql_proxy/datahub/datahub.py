from mindsdb.api.mysql.mysql_proxy.datahub.datanodes import InformationSchemaDataNode


def init_datahub(session, query, ml_handler):
    datahub = InformationSchemaDataNode(session, query, ml_handler)

    return datahub
