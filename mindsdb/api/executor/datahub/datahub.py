from mindsdb.api.executor.datahub.datanodes import InformationSchemaDataNode


def init_datahub(session):
    datahub = InformationSchemaDataNode(session)

    return datahub
