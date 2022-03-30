from mindsdb.interfaces.storage.db import session, Integration, View


class ViewController:
    def add(self, name, query, integration_name, company_id=None):
        integration_records = session.query(Integration).filter_by(company_id=company_id).all()
        integration_id = None
        for record in integration_records:
            if record.name == integration_name:
                integration_id = record.id
                break
        else:
            raise Exception(f"Can't find integration with name: {integration_name}")

        view_record = View(name=name, company_id=company_id, query=query, integration_id=integration_id)
        session.add(view_record)
        session.commit()

    def _get_view_record_data(self, record):
        return {
            'name': record.name,
            'query': record.query,
            'integration_id': record.integration_id
        }

    def get(self, id=None, name=None, company_id=None):
        if id is not None:
            records = session.query(View).filter_by(id=id, company_id=company_id).all()
        elif name is not None:
            records = session.query(View).filter_by(name=name, company_id=company_id).all()
        if len(records) == 0:
            raise Exception(f"Can't find view with name/id: {name}/{id}")
        elif len(records) > 1:
            raise Exception(f"There are multiple views with name/id: {name}/{id}")
        record = records[0]
        return self._get_view_record_data(record)

    def get_all(self, company_id=None):
        view_records = session.query(View).filter_by(company_id=company_id).all()
        views_dict = {}
        for record in view_records:
            views_dict[record.name] = self._get_view_record_data(record)
        return views_dict
