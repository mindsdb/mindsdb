from mindsdb.interfaces.storage import db
from mindsdb.interfaces.controllers.abc.collection import Collection
from mindsdb.interfaces.controllers.classes.database.integration_db import IntegrationDB
from mindsdb.utilities.context import context as ctx


class IntegrationCollection(Collection):
    def __init__(self):
        pass

    def all(self):
        integration_records = db.session.query(db.Integration).filter_by(company_id=ctx.company_id).all()
        integrations = [IntegrationDB.from_record(r) for r in integration_records]
        return integrations

    def __contains__(self, key):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def __getitem__(self, key):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        raise NotImplementedError()
