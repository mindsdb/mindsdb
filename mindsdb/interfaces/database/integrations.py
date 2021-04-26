from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Integration

def add_db_integration(name, data, company_id):
    if 'database_name' not in data:
        data['database_name'] = name
    if 'publish' not in data:
        data['publish'] = True

    integration_record = Integration(name=name, data=data, company_id=company_id)
    session.add(integration_record)
    session.commit()

def modify_db_integration(name, data, company_id):
    integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
    old_data = integration_record.data
    for k in old_data:
        if k not in data:
            data[k] = old_data[k]

    integration_record.data = data
    session.commit()

def remove_db_integration(name, company_id):
    session.query(Integration).filter_by(company_id=company_id, name=name).delete()
    session.commit()

def get_db_integration(name, company_id):
    integration_record = session.query(Integration).filter_by(company_id=company_id, name=name).first()
    if integration_record is None:
        return None
    return integration_record.data

def get_db_integrations(company_id):
    integration_records = session.query(Integration).filter_by(company_id=company_id).all()
    integration_dict = {}
    for record in integration_records:
        integration_dict[record.name] = record.data
    return integration_dict
