from mindsdb.api.mysql.mysql_proxy.datahub.datanodes import InformationSchemaDataNode


def init_datahub(session, ml_handler='lightwood'):
    datahub = InformationSchemaDataNode(session, ml_handler=ml_handler)

    return datahub
