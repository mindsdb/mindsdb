from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.parser.ast import Identifier
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.interfaces.storage.db import session, Integration, View


class ViewController:
    def add(self, name, query, integration_name, company_id=None):

        # check is name without paths
        # TODO or allow ?
        if len(name.split('.')) > 1:
            raise Exception(f'Name should be without dots: {name}')

        # name exists?
        rec = session.query(View.id).filter(View.name == name).first()
        if rec is not None:
            raise Exception(f'View already exists: {name}')

        integration_records = session.query(Integration).filter_by(company_id=company_id).all()

        if integration_name is not None:
            integration_id = None
            for record in integration_records:
                if record.name.lower() == integration_name.lower():
                    integration_id = record.id
                    break
            if integration_id is None:
                raise Exception(f"Can't find integration with name: {integration_name}")

            # inject integration into sql
            query_ast = parse_sql(query, dialect='mindsdb')

            def inject_integration(node, is_table, **kwargs):
                if is_table and isinstance(node, Identifier):
                    if not node.parts[0] == integration_name:
                        node.parts.insert(0, integration_name)

            query_traversal(query_ast, inject_integration)

            render = SqlalchemyRender('mysql')
            query = render.get_string(query_ast, with_failback=False)

        view_record = View(name=name, company_id=company_id, query=query)
        session.add(view_record)
        session.commit()

    def _get_view_record_data(self, record):

        return {
            'name': record.name,
            'query': record.query
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
