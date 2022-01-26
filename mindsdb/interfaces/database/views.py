from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Integration, View


class ViewController:
    def add(self, name, query, datasource_name, company_id=None):
        datasource_records = session.query(Integration).filter_by(company_id=company_id).all()
        datasource_id = None
        for record in datasource_records:
            if record.name == datasource_name:
                datasource_id = record.id
                break
        else:
            raise Exception(f"Can't find datasource with name: {datasource_name}")

        view_record = View(name=name, company_id=company_id, query=query, datasource_id=datasource_id)
        session.add(view_record)
        session.commit()

    def _get_view_record_data(self, record):
        return {
            'query': record.query,
            'datasource_id': record.datasource_id
        }

    def get_views(self, company_id=None):
        view_records = session.query(View).filter_by(company_id=company_id).all()
        views_dict = {}
        for record in view_records:
            views_dict[record.name] = self._get_view_record_data(record)
        return views_dict
