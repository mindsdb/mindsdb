import pandas as pd

from mindsdb_sql.parser.ast import Identifier, Select, OrderBy, NullConstant, Constant, BinaryOperation

from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx

from .last_query import LastQuery


class ContextController:

    def handle_db_context_vars(self, query, dn, session):

        l_query = LastQuery(query)
        if l_query.query is None:
            # no last keyword, exit
            return query, None

        query_str = l_query.to_string()
        context_name = self.get_context()

        rec = self.__get_context_record(context_name, query_str)

        if rec is None:
            values = self.__get_init_last_values(l_query, dn, session)
            self.__add_context_record(context_name, query_str, values)
        else:
            values = rec.values

        query_out = l_query.apply_values(values)

        def callback(data, columns_info):
            self._handle_results(l_query, context_name, query_str, data, columns_info)

        return query_out, callback

    def _handle_results(self, l_query, context_name, query_str, data, columns_info):

        if len(data) == 0:
            return

        max_vals = pd.DataFrame(data).max().to_dict()
        values = {}
        # get max values
        for info in l_query.get_last_columns():
            target_idx = info['target_idx']
            if target_idx is not None:
                # get by index
                col_name = columns_info[target_idx]['name']
                value = max_vals.get(col_name)
            else:
                # get by name
                value = max_vals.get(info['column_name'])

            if value is not None:
                values[info['table_name']] = {info['column_name']: value}

        self.__update_context_record(context_name, query_str, values)

    def drop_query_context(self, object_type, object_name):
        context_name = self.gen_context_name(object_type, object_name)
        for rec in db.session.query(db.QueryContext).filter_by(
            context_name=context_name,
            company_id=ctx.company_id
        ).all():
            db.session.delete(rec)
        db.session.commit()

    def __get_init_last_values(self, l_query, dn, session):
        """
        Get current last values for query
        """
        last_values = {}
        for info in l_query.get_last_columns():
            col = Identifier(info['column_name'])

            query = Select(
                targets=[
                    col
                ],
                from_table=info['table'],
                order_by=[
                    OrderBy(col, direction='DESC')
                ],
                where=BinaryOperation(
                    op='is not',
                    args=[
                        col,
                        NullConstant()
                    ]
                ),
                limit=Constant(1)
            )

            data, columns_info = dn.query(
                query=query,
                session=session
            )

            if len(data) == 0:
                value = None
            else:
                value = list(data[0].values())[0]
            last_values[info['table_name']] = {info['column_name']: value}
        return last_values

    # Context

    def get_context(self):
        try:
            return ctx.context_name
        except Exception:
            return ''

    def set_context(self, object_type, object_name):
        ctx.context_name = self.gen_context_name(object_type, object_name)

    def gen_context_name(self, object_type, object_name):
        return f'{object_type}-{object_name}'

    # DB
    def __get_context_record(self, context_name, query_str):

        return db.session.query(db.QueryContext).filter_by(
            query=query_str,
            context_name=context_name,
            company_id=ctx.company_id
        ).first()

    def __add_context_record(self, context_name, query_str, values):

        rec = db.QueryContext(
            query=query_str,
            context_name=context_name,
            company_id=ctx.company_id,
            values=values)
        db.session.add(rec)
        db.session.commit()
        return rec

    def __update_context_record(self, context_name, query_str, values):
        rec = self.__get_context_record(context_name, query_str)
        rec.values = values
        db.session.commit()


contextController = ContextController()
