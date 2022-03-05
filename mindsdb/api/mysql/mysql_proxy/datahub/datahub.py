from mindsdb.api.mysql.mysql_proxy.datahub.information_schema import InformationSchema


def init_datahub(session):
    datahub = InformationSchema(session)

    return datahub
