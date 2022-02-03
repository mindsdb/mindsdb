from mindsdb.api.mysql.mysql_proxy.datahub.information_schema import InformationSchema


# def init_datahub(model_interface, ai_table, data_store, datasource_interface, view_interface, company_id=None):
def init_datahub(session):
    datahub = InformationSchema(session)

    return datahub
