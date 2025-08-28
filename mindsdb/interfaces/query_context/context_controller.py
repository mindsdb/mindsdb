from typing import List, Optional, Iterable
import pickle
import datetime as dt

from sqlalchemy.orm.attributes import flag_modified
import pandas as pd

from mindsdb_sql_parser import Select, Star, OrderBy

from mindsdb_sql_parser.ast import Identifier, BinaryOperation, Last, Constant, ASTNode
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.utilities.cache import get_cache

from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.config import config

from .last_query import LastQuery


class RunningQuery:
    """
    Query in progres
    """

    OBJECT_TYPE = "query"

    def __init__(self, record: db.Queries):
        self.record = record
        self.sql = record.sql
        self.database = record.database or config.get("default_project")

    def get_partitions(self, dn, step_call, query: Select) -> Iterable:
        """
        Gets chunks of data from data handler for executing them in next steps of the planner
        Check if datanode supports fetch with stream
        :param dn: datanode to execute query
        :param step_call: instance of StepCall to get some parameters from it
        :param query: AST query to execute
        :return: generator with query results
        """
        if hasattr(dn, "has_support_stream") and dn.has_support_stream():
            query2 = self.get_partition_query(step_call.current_step_num, query, stream=True)

            for df in dn.query_stream(query2, fetch_size=self.batch_size):
                max_track_value = self.get_max_track_value(df)
                yield df
                self.set_progress(max_track_value=max_track_value)

        else:
            while True:
                query2 = self.get_partition_query(step_call.current_step_num, query, stream=False)

                response = dn.query(query=query2, session=step_call.session)
                df = response.data_frame

                if df is None or len(df) == 0:
                    break

                max_track_value = self.get_max_track_value(df)
                yield df
                self.set_progress(max_track_value=max_track_value)

    def get_partition_query(self, step_num: int, query: Select, stream=False) -> Select:
        """
        Generate query for fetching the next partition
        It wraps query to
           select * from ({query})
           where {track_column} > {previous_value}
           order by track_column
           limit size {batch_size}
        And fill track_column, previous_value, batch_size

        If steam is true:
          - if track_column is defined:
             - don't add limit
          - else:
             - return user query without modifications
        """

        track_column = self.record.parameters.get("track_column")
        if track_column is None and stream:
            # if no track column for stream fetching: it is not resumable query, execute original query

            # check if it is first run of the query
            if self.record.processed_rows > 0:
                raise RuntimeError("Can't resume query without track_column")
            return query

        if not stream and track_column is None:
            raise ValueError("Track column is not defined")

        query = Select(
            targets=[Star()],
            from_table=query,
            order_by=[OrderBy(Identifier(track_column))],
        )
        if not stream:
            query.limit = Constant(self.batch_size)

        track_value = self.record.context.get("track_value")
        # is it different step?
        cur_step_num = self.record.context.get("step_num")
        if cur_step_num is not None and cur_step_num != step_num:
            # reset track_value
            track_value = None
            self.record.context["track_value"] = None
            self.record.context["step_num"] = step_num
            flag_modified(self.record, "context")
            db.session.commit()

        if track_value is not None:
            query.where = BinaryOperation(
                op=">",
                args=[Identifier(track_column), Constant(track_value)],
            )

        return query

    def get_info(self):
        record = self.record
        return {
            "id": record.id,
            "sql": record.sql,
            "database": record.database,
            "started_at": record.started_at,
            "finished_at": record.finished_at,
            "parameters": record.parameters,
            "context": record.context,
            "processed_rows": record.processed_rows,
            "error": record.error,
            "updated_at": record.updated_at,
        }

    def add_to_task(self):
        task_record = db.Tasks(
            company_id=ctx.company_id,
            user_class=ctx.user_class,
            object_type=self.OBJECT_TYPE,
            object_id=self.record.id,
        )
        db.session.add(task_record)
        db.session.commit()

    def remove_from_task(self):
        task = db.Tasks.query.filter(
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.object_id == self.record.id,
            db.Tasks.company_id == ctx.company_id,
        ).first()

        if task is not None:
            db.session.delete(task)
            db.session.commit()

    def set_params(self, params: dict):
        """
        Store parameters of the step which is about to be split into partitions
        """

        if "batch_size" not in params:
            params["batch_size"] = 1000

        self.record.parameters = params
        self.batch_size = self.record.parameters["batch_size"]
        db.session.commit()

    def get_max_track_value(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        return max value to use in `set_progress`.
        this function is called before execution substeps,
         `set_progress` function - after
        """
        if "track_column" in self.record.parameters:
            track_column = self.record.parameters["track_column"]
            return df[track_column].max()
        else:
            # stream mode
            return None

    def set_progress(self, processed_rows: int = None, max_track_value: int = None):
        """
        Store progres of the query, it is called after processing of batch
        """

        if processed_rows is not None and processed_rows > 0:
            self.record.processed_rows = self.record.processed_rows + processed_rows
            db.session.commit()

        if max_track_value is not None:
            cur_value = self.record.context.get("track_value")
            new_value = max_track_value
            if new_value is not None:
                if cur_value is None or new_value > cur_value:
                    self.record.context["track_value"] = new_value
                    flag_modified(self.record, "context")
            db.session.commit()

    def on_error(self, error: Exception, step_num: int, steps_data: dict):
        """
        Saves error of the query in database
        Also saves step data and current step num to be able to resume query
        """
        self.record.error = str(error)
        self.record.context["step_num"] = step_num
        flag_modified(self.record, "context")

        # save steps_data
        cache = get_cache("steps_data")
        data = pickle.dumps(steps_data, protocol=5)
        cache.set(str(self.record.id), data)

        db.session.commit()

    def mark_as_run(self):
        """
        Mark query as running and reset error of the query
        """
        if self.record.finished_at is not None:
            raise RuntimeError("The query already finished")

        if self.record.started_at is None:
            self.record.started_at = dt.datetime.now()
            db.session.commit()
        elif self.record.error is not None:
            self.record.error = None
            db.session.commit()
        else:
            raise RuntimeError("The query might be running already")

    def get_state(self) -> dict:
        """
        Returns stored state for resuming the query
        """
        cache = get_cache("steps_data")
        key = self.record.id
        data = cache.get(key)
        cache.delete(key)

        steps_data = pickle.loads(data)

        return {
            "step_num": self.record.context.get("step_num"),
            "steps_data": steps_data,
        }

    def finish(self):
        """
        Mark query as finished
        """

        self.record.finished_at = dt.datetime.now()
        db.session.commit()


class QueryContextController:
    IGNORE_CONTEXT = "<IGNORE>"

    def handle_db_context_vars(self, query: ASTNode, dn, session) -> tuple:
        """
        Check context variables in query and replace them with values.
        Should be used before exec query in database.

        Input:
        - query: input query
        - params are used to find current values of context variables
          - dn: datanode
          - session: mindsdb server session

        Returns:
         - query with replaced context variables
         - callback to call with result of the query. it is used to update context variables

        """
        context_name = self.get_current_context()

        l_query = LastQuery(query)
        if l_query.query is None:
            # no last keyword, exit
            return query, None

        if context_name == self.IGNORE_CONTEXT:
            # return with empty constants
            return l_query.query, None

        query_str = l_query.to_string()

        rec = self._get_context_record(context_name, query_str)

        if rec is None or len(rec.values) == 0:
            values = self._get_init_last_values(l_query, dn, session)
            if rec is None:
                self.__add_context_record(context_name, query_str, values)
                if context_name.startswith("job-if-"):
                    # add context for job also
                    self.__add_context_record(context_name.replace("job-if", "job"), query_str, values)
            else:
                rec.values = values
        else:
            values = rec.values

        db.session.commit()

        query_out = l_query.apply_values(values)

        def callback(df, columns_info):
            self._result_callback(l_query, context_name, query_str, df, columns_info)

        return query_out, callback

    def remove_lasts(self, query):
        def replace_lasts(node, **kwargs):
            # find last in where
            if isinstance(node, BinaryOperation):
                if isinstance(node.args[0], Identifier) and isinstance(node.args[1], Last):
                    node.args = [Constant(0), Constant(0)]
                    node.op = "="

        # find lasts
        query_traversal(query, replace_lasts)
        return query

    def _result_callback(
        self, l_query: LastQuery, context_name: str, query_str: str, df: pd.DataFrame, columns_info: list
    ):
        """
        This function handlers result from executed query and updates context variables with new values

        Input
        - l_query: LastQuery object
        - To identify context:
          - context_name: name of the context
          - query_str: rendered query to search in context table
        - result of the query
          - data: list of dicts
          - columns_info: list

        """
        if len(df) == 0:
            return

        values = {}
        # get max values
        for info in l_query.get_last_columns():
            target_idx = info["target_idx"]
            if target_idx is not None:
                # get by index
                col_name = columns_info[target_idx]["name"]
            else:
                col_name = info["column_name"]
                # get by name
            if col_name not in df:
                continue

            column_values = df[col_name].dropna()
            try:
                value = max(column_values)
            except (TypeError, ValueError):
                try:
                    # try to convert to float
                    value = max(map(float, column_values))
                except (TypeError, ValueError):
                    try:
                        # try to convert to str
                        value = max(map(str, column_values))
                    except (TypeError, ValueError):
                        continue

            if value is not None:
                values[info["table_name"]] = {info["column_name"]: value}

        self.__update_context_record(context_name, query_str, values)

    def drop_query_context(self, object_type: str, object_id: int = None):
        """
        Drop context for object
        :param object_type: type of the object
        :param object_id: id
        """

        context_name = self.gen_context_name(object_type, object_id)
        for rec in (
            db.session.query(db.QueryContext).filter_by(context_name=context_name, company_id=ctx.company_id).all()
        ):
            db.session.delete(rec)
        db.session.commit()

    def _get_init_last_values(self, l_query: LastQuery, dn, session) -> dict:
        """
        Gets current last values for query.
        Creates and executes query for it:
           'select <col> from <table> order by <col> desc limit 1"
        """
        last_values = {}
        for query, info in l_query.get_init_queries():
            response = dn.query(query=query, session=session)
            data = response.data_frame
            columns_info = response.columns

            if len(data) == 0:
                value = None
            else:
                row = list(data.iloc[0])

                idx = None
                for i, col in enumerate(columns_info):
                    if col["name"].upper() == info["column_name"].upper():
                        idx = i
                        break

                if idx is None or len(row) == 1:
                    value = row[0]
                else:
                    value = row[idx]

            if value is not None:
                last_values[info["table_name"]] = {info["column_name"]: value}

        return last_values

    # Context

    def get_current_context(self) -> str:
        """
        returns current context name
        """
        try:
            context_stack = ctx.context_stack or []
        except AttributeError:
            context_stack = []
        if len(context_stack) > 0:
            return context_stack[-1]
        else:
            return ""

    def set_context(self, object_type: str = None, object_id: int = None):
        """
        Updates current context name, using object name and id
        Previous context names are stored on lower levels of stack
        """
        try:
            context_stack = ctx.context_stack or []
        except AttributeError:
            context_stack = []
        context_stack.append(self.gen_context_name(object_type, object_id))
        ctx.context_stack = context_stack

    def release_context(self, object_type: str = None, object_id: int = None):
        """
        Removed current context (defined by object type and id) and restored previous one
        """
        try:
            context_stack = ctx.context_stack or []
        except AttributeError:
            context_stack = []
        if len(context_stack) == 0:
            return
        context_name = self.gen_context_name(object_type, object_id)
        if context_stack[-1] == context_name:
            context_stack.pop()
        ctx.context_stack = context_stack

    def gen_context_name(self, object_type: str, object_id: int) -> str:
        """
        Generated name of the context according to object type and name
        :return: context name
        """

        if object_type is None:
            return ""
        if object_id is not None:
            object_type += "-" + str(object_id)
        return object_type

    def get_context_vars(self, object_type: str, object_id: int) -> List[dict]:
        """
        Return variables stored in context (defined by object type and id)

        :return: list of all context variables related to context name how they stored in context table
        """
        context_name = self.gen_context_name(object_type, object_id)
        vars = []
        for rec in db.session.query(db.QueryContext).filter_by(context_name=context_name, company_id=ctx.company_id):
            if rec.values is not None:
                vars.append(rec.values)

        return vars

    # DB
    def _get_context_record(self, context_name: str, query_str: str) -> db.QueryContext:
        """
        Find and return record for context and query string
        """

        return (
            db.session.query(db.QueryContext)
            .filter_by(query=query_str, context_name=context_name, company_id=ctx.company_id)
            .first()
        )

    def __add_context_record(self, context_name: str, query_str: str, values: dict) -> db.QueryContext:
        """
        Creates record (for context and query string) with values and returns it
        """
        rec = db.QueryContext(query=query_str, context_name=context_name, company_id=ctx.company_id, values=values)
        db.session.add(rec)
        return rec

    def __update_context_record(self, context_name: str, query_str: str, values: dict):
        """
        Updates context record with new values
        """
        rec = self._get_context_record(context_name, query_str)
        rec.values = values
        db.session.commit()

    def get_query(self, query_id: int) -> RunningQuery:
        """
        Get running query by id
        """

        rec = db.Queries.query.filter(db.Queries.id == query_id, db.Queries.company_id == ctx.company_id).first()

        if rec is None:
            raise RuntimeError(f"Query not found: {query_id}")
        return RunningQuery(rec)

    def create_query(self, query: ASTNode, database: str = None) -> RunningQuery:
        """
        Create a new running query from AST query
        """

        # remove old queries
        remove_query = db.session.query(db.Queries).filter(
            db.Queries.company_id == ctx.company_id, db.Queries.finished_at < (dt.datetime.now() - dt.timedelta(days=1))
        )
        for rec in remove_query.all():
            self.get_query(rec.id).remove_from_task()
            db.session.delete(rec)

        rec = db.Queries(
            sql=str(query),
            database=database,
            company_id=ctx.company_id,
        )

        db.session.add(rec)
        db.session.commit()
        return RunningQuery(rec)

    def list_queries(self) -> List[dict]:
        """
        Get list of all running queries with metadata
        """

        query = db.session.query(db.Queries).filter(db.Queries.company_id == ctx.company_id)
        return [RunningQuery(record).get_info() for record in query]

    def cancel_query(self, query_id: int):
        """
        Cancels running query by id
        """
        rec = db.Queries.query.filter(db.Queries.id == query_id, db.Queries.company_id == ctx.company_id).first()
        if rec is None:
            raise RuntimeError(f"Query not found: {query_id}")

        self.get_query(rec.id).remove_from_task()

        # the query in progress will fail when it tries to update status
        db.session.delete(rec)
        db.session.commit()


query_context_controller = QueryContextController()
