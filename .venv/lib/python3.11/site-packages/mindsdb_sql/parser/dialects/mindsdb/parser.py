from sly import Parser
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.ast.drop import DropDatabase, DropView
from mindsdb_sql.parser.dialects.mindsdb.agents import CreateAgent, DropAgent, UpdateAgent
from mindsdb_sql.parser.dialects.mindsdb.drop_datasource import DropDatasource
from mindsdb_sql.parser.dialects.mindsdb.drop_predictor import DropPredictor
from mindsdb_sql.parser.dialects.mindsdb.drop_dataset import DropDataset
from mindsdb_sql.parser.dialects.mindsdb.drop_ml_engine import DropMLEngine
from mindsdb_sql.parser.dialects.mindsdb.create_predictor import CreatePredictor, CreateAnomalyDetectionModel
from mindsdb_sql.parser.dialects.mindsdb.create_database import CreateDatabase
from mindsdb_sql.parser.dialects.mindsdb.create_ml_engine import CreateMLEngine
from mindsdb_sql.parser.dialects.mindsdb.create_view import CreateView
from mindsdb_sql.parser.dialects.mindsdb.create_job import CreateJob
from mindsdb_sql.parser.dialects.mindsdb.chatbot import CreateChatBot, UpdateChatBot, DropChatBot
from mindsdb_sql.parser.dialects.mindsdb.drop_job import DropJob
from mindsdb_sql.parser.dialects.mindsdb.trigger import CreateTrigger, DropTrigger
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.parser.dialects.mindsdb.evaluate import Evaluate
from mindsdb_sql.parser.dialects.mindsdb.knowledge_base import CreateKnowledgeBase, DropKnowledgeBase
from mindsdb_sql.parser.dialects.mindsdb.skills import CreateSkill, DropSkill, UpdateSkill
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer
from mindsdb_sql.parser.dialects.mindsdb.retrain_predictor import RetrainPredictor
from mindsdb_sql.parser.dialects.mindsdb.finetune_predictor import FinetunePredictor
from mindsdb_sql.parser.logger import ParserLogger
from mindsdb_sql.parser.utils import ensure_select_keyword_order, JoinType, tokens_to_string

all_tokens_list = MindsDBLexer.tokens.copy()
all_tokens_list.remove('RPAREN')
all_tokens_list.remove('LPAREN')

"""
Unfortunately the rules are not iherited from base SQLParser, because it just doesn't work with Sly due to metaclass magic.
"""


class MindsDBParser(Parser):
    log = ParserLogger()
    tokens = MindsDBLexer.tokens

    precedence = (
        ('left', OR),
        ('left', AND),
        ('right', UNOT),
        ('left', EQUALS, NEQUALS),
        ('nonassoc', LESS, LEQ, GREATER, GEQ, IN, BETWEEN, IS, IS_NOT, NOT_LIKE, LIKE),
        ('left', JSON_GET),
        ('left', PLUS, MINUS),
        ('left', STAR, DIVIDE),
        ('right', UMINUS),  # Unary minus operator, unary not

    )

    # Top-level statements
    @_('show',
       'start_transaction',
       'commit_transaction',
       'rollback_transaction',
       'alter_table',
       'explain',
       'set',
       'use',
       'describe',
       'create_predictor',
       'create_integration',
       'create_view',
       'create_anomaly_detection_model',
       'drop_predictor',
       'drop_datasource',
       'drop_dataset',
       'union',
       'select',
       'insert',
       'update',
       'delete',
       'evaluate',
       'drop_database',
       'drop_view',
       'drop_table',
       'create_table',
       'create_job',
       'drop_job',
       'create_chat_bot',
       'drop_chat_bot',
       'update_chat_bot',
       'create_trigger',
       'drop_trigger',
       'create_kb',
       'drop_kb',
       'create_skill',
       'drop_skill',
       'update_skill',
       'create_agent',
       'drop_agent',
       'update_agent'
       )
    def query(self, p):
        return p[0]

    # -- Knowledge Base --
    @_(
        'CREATE KNOWLEDGE_BASE if_not_exists_or_empty identifier USING kw_parameter_list',
        'CREATE KNOWLEDGE_BASE if_not_exists_or_empty identifier',
        # from select
        'CREATE KNOWLEDGE_BASE if_not_exists_or_empty identifier FROM LPAREN select RPAREN USING kw_parameter_list',
        'CREATE KNOWLEDGE_BASE if_not_exists_or_empty identifier FROM LPAREN select RPAREN',
    )
    def create_kb(self, p):
        params = getattr(p, 'kw_parameter_list', {})
        from_query = getattr(p, 'select', None)
        name = p.identifier
        # check model and storage are in params
        params = {k.lower(): v for k, v in params.items()}  # case insensitive
        model = params.pop('model', None)
        storage = params.pop('storage', None)

        if isinstance(model, str):
            # convert to identifier
            storage = Identifier(storage)

        if isinstance(model, str):
            # convert to identifier
            model = Identifier(model)

        if_not_exists = p.if_not_exists_or_empty

        return CreateKnowledgeBase(
            name=name,
            model=model,
            storage=storage,
            from_select=from_query,
            params=params,
            if_not_exists=if_not_exists
        )

    @_('DROP KNOWLEDGE_BASE if_exists_or_empty identifier')
    def drop_kb(self, p):
        return DropKnowledgeBase(name=p.identifier, if_exists=p.if_exists_or_empty)

    # -- Skills --
    @_('CREATE SKILL if_not_exists_or_empty identifier USING kw_parameter_list')
    def create_skill(self, p):
        params = p.kw_parameter_list

        return CreateSkill(
            name=p.identifier,
            type=params.pop('type'),
            params=params,
            if_not_exists=p.if_not_exists_or_empty
        )

    @_('DROP SKILL if_exists_or_empty identifier')
    def drop_skill(self, p):
        return DropSkill(name=p.identifier, if_exists=p.if_exists_or_empty)

    @_('UPDATE SKILL identifier SET kw_parameter_list')
    def update_skill(self, p):
        return UpdateSkill(name=p.identifier, updated_params=p.kw_parameter_list)

    # -- Agent --
    @_('CREATE AGENT if_not_exists_or_empty identifier USING kw_parameter_list')
    def create_agent(self, p):
        params = p.kw_parameter_list

        return CreateAgent(
            name=p.identifier,
            model=params.pop('model'),
            params=params,
            if_not_exists=p.if_not_exists_or_empty
        )
    
    @_('DROP AGENT if_exists_or_empty identifier')
    def drop_agent(self, p):
        return DropAgent(name=p.identifier, if_exists=p.if_exists_or_empty)
    
    @_('UPDATE AGENT identifier SET kw_parameter_list')
    def update_agent(self, p):
        return UpdateAgent(name=p.identifier, updated_params=p.kw_parameter_list)

    # -- ChatBot --
    @_('CREATE CHATBOT identifier USING kw_parameter_list')
    def create_chat_bot(self, p):
        params = p.kw_parameter_list

        database = Identifier(params.pop('database'))
        model_param = params.pop('model', None)
        agent_param = params.pop('agent', None)
        model = Identifier(
            model_param) if model_param is not None else None
        agent = Identifier(
            agent_param) if agent_param is not None else None
        return CreateChatBot(
            name=p.identifier,
            database=database,
            model=model,
            agent=agent,
            params=params
        )

    @_('UPDATE CHATBOT identifier SET kw_parameter_list')
    def update_chat_bot(self, p):
        return UpdateChatBot(name=p.identifier, updated_params=p.kw_parameter_list)

    @_('DROP CHATBOT identifier')
    def drop_chat_bot(self, p):
        return DropChatBot(name=p.identifier)

    # -- triggers --
    @_('CREATE TRIGGER identifier ON identifier LPAREN raw_query RPAREN')
    @_('CREATE TRIGGER identifier ON identifier COLUMNS ordering_terms LPAREN raw_query RPAREN')
    def create_trigger(self, p):
        query_str = tokens_to_string(p.raw_query)

        columns = None
        if hasattr(p, 'ordering_terms'):
            columns = [i.field for i in p.ordering_terms]

        return CreateTrigger(
            name=p.identifier0,
            table=p.identifier1,
            query_str=query_str,
            columns=columns
        )

    @_('DROP TRIGGER identifier')
    def drop_trigger(self, p):
        return DropTrigger(name=p.identifier)

    # -- Jobs --
    @_('CREATE JOB if_not_exists_or_empty identifier LPAREN raw_query RPAREN job_schedule',
       'CREATE JOB if_not_exists_or_empty identifier AS LPAREN raw_query RPAREN job_schedule',
       'CREATE JOB if_not_exists_or_empty identifier LPAREN raw_query RPAREN job_schedule IF LPAREN raw_query RPAREN',
       'CREATE JOB if_not_exists_or_empty identifier AS LPAREN raw_query RPAREN job_schedule IF LPAREN raw_query RPAREN',
       'CREATE JOB if_not_exists_or_empty identifier LPAREN raw_query RPAREN',
       'CREATE JOB if_not_exists_or_empty identifier AS LPAREN raw_query RPAREN',
       'CREATE JOB if_not_exists_or_empty identifier LPAREN raw_query RPAREN IF LPAREN raw_query RPAREN',
       'CREATE JOB if_not_exists_or_empty identifier AS LPAREN raw_query RPAREN IF LPAREN raw_query RPAREN'
       )
    def create_job(self, p):
        if hasattr(p, 'raw_query0'):
            query_str = tokens_to_string(p.raw_query0)
            if_query_str = tokens_to_string(p.raw_query1)
        else:
            query_str = tokens_to_string(p.raw_query)
            if_query_str = None

        job_schedule = getattr(p, 'job_schedule', {})

        start_str = None
        if 'START' in job_schedule:
            start_str = job_schedule.pop('START')

        end_str = None
        if 'END' in job_schedule:
            end_str = job_schedule.pop('END')

        repeat_str = None
        if 'EVERY' in job_schedule:
            repeat_str = job_schedule.pop('EVERY')

        if len(job_schedule) > 0:
            raise ParsingException(f'Unexpected params: {list(job_schedule.keys())}')

        return CreateJob(
            name=p.identifier,
            query_str=query_str,
            if_query_str=if_query_str,
            start_str=start_str,
            end_str=end_str,
            repeat_str=repeat_str,
            if_not_exists=p.if_not_exists_or_empty
        )

    @_('START string',
       'START id',
       'END string',
       'EVERY string',
       'EVERY id',
       'EVERY integer id',
       'job_schedule job_schedule')
    def job_schedule(self, p):

        if isinstance(p[0], dict):
            schedule = p[0]
            for k in p[1].keys():
                if k in p[0]:
                    raise ParsingException(f'Duplicated param: {k}')

            schedule.update(p[1])
            return schedule

        param = p[0].upper()
        value = p[1]
        if param == 'EVERY':
            # 'integer + id' mode
            if hasattr(p, 'integer'):
                value = f'{p[1]} {p[2]}'

        schedule = {param:value}
        return schedule

    @_('DROP JOB if_exists_or_empty identifier')
    def drop_job(self, p):
        return DropJob(name=p.identifier, if_exists=p.if_exists_or_empty)

    # Explain
    @_('EXPLAIN identifier')
    def explain(self, p):
        return Explain(target=p.identifier)

    # Alter table
    @_('ALTER TABLE identifier id id')
    def alter_table(self, p):
        return AlterTable(target=p.identifier,
                          arg=' '.join([p.id0, p.id1]))

    # DROP VEW
    @_('DROP VIEW if_exists_or_empty identifier')
    def drop_view(self, p):
        return DropView([p.identifier], if_exists=p.if_exists_or_empty)

    @_('DROP VIEW if_exists_or_empty enumeration')
    def drop_view(self, p):
        return DropView(p.enumeration, if_exists=p.if_exists_or_empty)

    # DROP DATABASE
    @_('DROP DATABASE if_exists_or_empty identifier',
       'DROP PROJECT if_exists_or_empty identifier',
       'DROP SCHEMA if_exists_or_empty identifier')
    def drop_database(self, p):
        return DropDatabase(name=p.identifier, if_exists=p.if_exists_or_empty)

    # Transactions

    @_('START TRANSACTION',
       'BEGIN')
    def start_transaction(self, p):
        # https://dev.mysql.com/doc/refman/8.0/en/commit.html
        return StartTransaction()

    @_('COMMIT')
    def commit_transaction(self, p):
        return CommitTransaction()

    @_('ROLLBACK')
    def rollback_transaction(self, p):
        return RollbackTransaction()

    # --- Set ---
    @_('SET set_item_list')
    def set(self, p):
        set_list = p[1]
        if len(set_list) == 1:
            return set_list[0]
        return Set(set_list=set_list)

    @_('set_item',
       'set_item_list COMMA set_item')
    def set_item_list(self, p):
        arr = getattr(p, 'set_item_list', [])
        arr.append(p.set_item)
        return arr

    # set names
    @_('id id',
       'id constant',
       'id identifier',
       'id id COLLATE constant',
       'id id COLLATE id',
       'id constant COLLATE constant',
       'id constant COLLATE id')
    def set_item(self, p):
        category = p[0]

        if isinstance(p[1], (Constant, Identifier)):
            value = p[1]
        else:
            # is id
            value = Constant(p[1], with_quotes=False)

        params = {}
        if hasattr(p, 'COLLATE'):
            if category.lower() != 'names':
                raise ParsingException(f'Expected "SET names", got "SET {category}"')

            if isinstance(p[3], Constant):
                val = p[3]
            else:
                val = Constant(p[3], with_quotes=False)
            params['COLLATE'] = val

        return Set(category=category, value=value, params=params)

    # set charset
    @_('charset constant',
       'charset id')
    def set_item(self, p):
        if hasattr(p, 'id'):
            arg = Constant(p.id, with_quotes=False)
        else:
            arg = p.constant
        return Set(category='CHARSET', value=arg)

    @_('CHARACTER SET',
       'CHARSET',
       )
    def charset(self, p):
        if hasattr(p, 'SET'):
            return f'{p[0]} {p[1]}'
        return p[0]

    # set transaction
    @_('set_scope TRANSACTION transact_property_list',
       'TRANSACTION transact_property_list')
    def set_item(self, p):
        isolation_level = None
        access_mode = None
        transact_scope = getattr(p, 'set_scope', None)
        for prop in p.transact_property_list:
            if prop['type'] == 'iso_level':
                isolation_level = prop['value']
            else:
                access_mode = prop['value']

        params = {}
        if isolation_level is not None:
            params['isolation level'] = isolation_level
        if access_mode is not None:
            params['access_mode'] = access_mode

        return Set(
            category='TRANSACTION',
            scope=transact_scope,
            params=params
        )

    @_('transact_property_list COMMA transact_property')
    def transact_property_list(self, p):
        return p.transact_property_list + [p.transact_property]

    @_('transact_property')
    def transact_property_list(self, p):
        return [p[0]]

    @_('ISOLATION LEVEL transact_level',
       'transact_access_mode')
    def transact_property(self, p):
        if hasattr(p, 'transact_level'):
            return {'type':'iso_level', 'value':p.transact_level}
        else:
            return {'type':'access_mode', 'value':p.transact_access_mode}

    @_('REPEATABLE READ',
       'READ COMMITTED',
       'READ UNCOMMITTED',
       'SERIALIZABLE')
    def transact_level(self, p):
        return ' '.join([x for x in p])

    @_('READ WRITE',
       'READ ONLY')
    def transact_access_mode(self, p):
        return ' '.join([x for x in p])

    @_('identifier EQUALS expr',
       'set_scope identifier EQUALS expr',
       'variable EQUALS expr',
       'set_scope variable EQUALS expr')
    def set_item(self, p):

        scope = None
        name = p[0]
        if hasattr(p, 'set_scope'):
            scope = p.set_scope
            name=p[1]

        return Set(name=name, value=p.expr, scope=scope)

    @_('GLOBAL',
       'PERSIST',
       'PERSIST_ONLY',
       'SESSION',
       )
    def set_scope(self, p):
        return p[0]

    # --- Show ---
    @_('show WHERE expr')
    def show(self, p):
        command = p.show
        command.where = p.expr
        return command

    @_('show LIKE string')
    def show(self, p):
        command = p.show
        command.like = p.string
        return command

    @_('show FROM identifier')
    def show(self, p):
        command = p.show
        value0 = command.from_table
        value1 = p.identifier
        if value0 is not None:
            value1.parts = value1.parts + value0.parts

        command.from_table = value1
        return command

    @_('show IN identifier')
    def show(self, p):
        command = p.show
        value0 = command.in_table
        value1 = p.identifier
        if value0 is not None:
            value1.parts = value1.parts + value0.parts

        command.in_table = value1
        return command

    @_('SHOW show_category',
       'SHOW show_modifier_list show_category')
    def show(self, p):
        modes = getattr(p, 'show_modifier_list', None)
        return Show(
            category=p.show_category,
            modes=modes
        )

    @_(
       'id',
       'id id',
    )
    def show_category(self, p):
        if hasattr(p, 'id'):
            return p.id
        return f"{p.id0} {p.id1}"

    # custom show commands

    @_('SHOW id id identifier')
    def show(self, p):
        category = p[1] + ' ' + p[2]

        if p[1].lower() == 'engine':
            name = p.identifier.parts[0]
        else:
            name = p.identifier.to_string()
        return Show(
            category=category,
            name=name
        )

    @_('SHOW REPLICA STATUS FOR CHANNEL id',
       'SHOW SLAVE STATUS FOR CHANNEL id',
       'SHOW REPLICA STATUS',
       'SHOW SLAVE STATUS', )
    def show(self, p):
        name = getattr(p, 'id', None)
        return Show(
            category='REPLICA STATUS',  # slave = replica
            name=name
        )

    @_('show_modifier',
       'show_modifier_list show_modifier')
    def show_modifier_list(self, p):
        if hasattr(p, 'empty'):
            return None
        params = getattr(p, 'show_modifier_list', [])
        params.append(p.show_modifier)
        return params

    @_('EXTENDED',
       'FULL')
    def show_modifier(self, p):
        return p[0]

    # DELETE
    @_('DELETE FROM identifier WHERE expr',
       'DELETE FROM identifier')
    def delete(self, p):
        where = getattr(p, 'expr', None)

        if where is not None and not isinstance(where, Operation):
            raise ParsingException(
                f"WHERE must contain an operation that evaluates to a boolean, got: {str(where)}")

        return Delete(table=p.identifier, where=where)

    # UPDATE
    @_('UPDATE identifier SET update_parameter_list FROM LPAREN select RPAREN AS id WHERE expr',
       'UPDATE identifier SET update_parameter_list WHERE expr',
       'UPDATE identifier SET update_parameter_list')
    def update(self, p):
        where = getattr(p, 'expr', None)
        from_select = getattr(p, 'select', None)
        from_select_alias = getattr(p, 'id', None)
        if from_select_alias is not None:
            from_select_alias = Identifier(from_select_alias)
        return Update(table=p.identifier,
                      update_columns=p.update_parameter_list,
                      from_select=from_select,
                      from_select_alias=from_select_alias,
                      where=where)

    # UPDATE
    @_('UPDATE identifier ON ordering_terms FROM LPAREN select RPAREN')
    def update(self, p):
        keys = [i.field for i in p.ordering_terms]
        return Update(table=p.identifier,
                      keys=keys,
                      from_select=p.select)

    # INSERT
    @_('INSERT INTO identifier LPAREN result_columns RPAREN select',
       'INSERT INTO identifier select')
    def insert(self, p):
        columns = getattr(p, 'result_columns', None)
        return Insert(table=p.identifier, columns=columns, from_select=p.select)

    @_('INSERT INTO identifier LPAREN result_columns RPAREN VALUES expr_list_set',
       'INSERT INTO identifier VALUES expr_list_set')
    def insert(self, p):
        columns = getattr(p, 'result_columns', None)
        return Insert(table=p.identifier, columns=columns, values=p.expr_list_set)

    @_('expr_list_set COMMA expr_list_set')
    def expr_list_set(self, p):
        return p.expr_list_set0 + p.expr_list_set1

    @_('LPAREN expr_list RPAREN')
    def expr_list_set(self, p):
        return [p.expr_list]

    # DESCRIBE

    @_('DESCRIBE identifier')
    def describe(self, p):
        return Describe(value=p.identifier)

    @_('DESCRIBE JOB identifier',
       'DESCRIBE SKILL identifier',
       'DESCRIBE CHATBOT identifier',
       'DESCRIBE TRIGGER identifier',
       'DESCRIBE KNOWLEDGE_BASE identifier',
       'DESCRIBE PROJECT identifier',
       'DESCRIBE ML_ENGINE identifier',
       'DESCRIBE identifier identifier',
       )
    def describe(self, p):
        if isinstance(p[1], Identifier):
            type = p[1].parts[-1]
        else:
            type = p[1]
        type = type.replace(' ', '_')
        return Describe(value=p[2], type=type)


    # USE

    @_('USE identifier')
    def use(self, p):
        return Use(value=p.identifier)

    # CREATE VIEW
    @_('CREATE VIEW if_not_exists_or_empty identifier create_view_from_table_or_nothing AS LPAREN raw_query RPAREN',
       'CREATE VIEW if_not_exists_or_empty identifier create_view_from_table_or_nothing LPAREN raw_query RPAREN')
    def create_view(self, p):
        query_str = tokens_to_string(p.raw_query)

        return CreateView(name=p.identifier,
                          from_table=p.create_view_from_table_or_nothing,
                          query_str=query_str,
                          if_not_exists=p.if_not_exists_or_empty)

    @_('FROM identifier')
    def create_view_from_table_or_nothing(self, p):
        return p.identifier

    @_('empty')
    def create_view_from_table_or_nothing(self, p):
        pass

    # DROP PREDICTOR
    @_('DROP PREDICTOR if_exists_or_empty identifier',
       'DROP MODEL if_exists_or_empty identifier')
    def drop_predictor(self, p):
        return DropPredictor(p.identifier, if_exists=p.if_exists_or_empty)

    # DROP DATASOURCE
    @_('DROP DATASOURCE if_exists_or_empty identifier')
    def drop_datasource(self, p):
        return DropDatasource(p.identifier, if_exists=p.if_exists_or_empty)

    # DROP DATASET
    @_('DROP DATASET if_exists_or_empty identifier')
    def drop_dataset(self, p):
        return DropDataset(p.identifier, if_exists=p.if_exists_or_empty)

    # DROP TABLE
    @_('DROP TABLE if_exists_or_empty identifier')
    def drop_table(self, p):
        return DropTables(tables=[p.identifier], if_exists=p.if_exists_or_empty)

    # create table
    @_('id id',
       'id id LPAREN INTEGER RPAREN')
    def table_column(self, p):
        return TableColumn(
            name=p[0],
            type=p[1],
            length=getattr(p, 'INTEGER', None)
        )

    @_('table_column',
       'table_column_list COMMA table_column')
    def table_column_list(self, p):
        items = getattr(p, 'table_column_list', [])
        items.append(p.table_column)
        return items

    @_('CREATE replace_or_empty TABLE if_not_exists_or_empty identifier LPAREN table_column_list RPAREN')
    def create_table(self, p):
        return CreateTable(
            name=p.identifier,
            columns=p.table_column_list,
            is_replace=getattr(p, 'replace_or_empty', False),
            if_not_exists=getattr(p, 'if_not_exists_or_empty', False)
        )

    @_(
       'CREATE replace_or_empty TABLE if_not_exists_or_empty identifier select',
       'CREATE replace_or_empty TABLE if_not_exists_or_empty identifier LPAREN select RPAREN',
    )
    def create_table(self, p):
        is_replace = getattr(p, 'replace_or_empty', False)

        return CreateTable(
            name=p.identifier,
            is_replace=is_replace,
            from_select=p.select,
            if_not_exists=getattr(p, 'if_not_exists_or_empty', False)
        )

    # create predictor

    @_('create_predictor USING kw_parameter_list')
    def create_predictor(self, p):
        p.create_predictor.using = p.kw_parameter_list
        return p.create_predictor

    @_('create_predictor HORIZON integer')
    def create_predictor(self, p):
        p.create_predictor.horizon = p.integer
        return p.create_predictor

    @_('create_predictor WINDOW integer')
    def create_predictor(self, p):
        p.create_predictor.window = p.integer
        return p.create_predictor

    @_('create_predictor GROUP_BY expr_list')
    def create_predictor(self, p):
        group_by = p.expr_list
        if not isinstance(group_by, list):
            group_by = [group_by]

        p.create_predictor.group_by = group_by
        return p.create_predictor

    @_('create_predictor ORDER_BY ordering_terms')
    def create_predictor(self, p):
        p.create_predictor.order_by = p.ordering_terms
        return p.create_predictor

    @_('CREATE replace_or_empty PREDICTOR if_not_exists_or_empty identifier FROM identifier LPAREN raw_query RPAREN PREDICT result_columns',
       'CREATE replace_or_empty PREDICTOR if_not_exists_or_empty identifier PREDICT result_columns',
       'CREATE replace_or_empty MODEL if_not_exists_or_empty identifier FROM identifier LPAREN raw_query RPAREN PREDICT result_columns',
       'CREATE replace_or_empty MODEL if_not_exists_or_empty identifier FROM LPAREN raw_query RPAREN PREDICT result_columns',
       'CREATE replace_or_empty MODEL if_not_exists_or_empty identifier PREDICT result_columns'
       )
    def create_predictor(self, p):
        query_str = None
        if hasattr(p, 'raw_query'):
            query_str = tokens_to_string(p.raw_query)

        if hasattr(p, 'identifier'):
            # single identifier field
            name = p.identifier
        else:
            name = p.identifier0

        return CreatePredictor(
            name=name,
            integration_name=getattr(p, 'identifier1', None),
            query_str=query_str,
            targets=p.result_columns,
            if_not_exists=p.if_not_exists_or_empty,
            is_replace=p.replace_or_empty
        )

    # Typed models
    ## Anomaly detection
    @_(
        'CREATE ANOMALY DETECTION MODEL identifier',  # for methods that do not require training (e.g. TimeGPT)
        'CREATE ANOMALY DETECTION MODEL identifier FROM identifier LPAREN raw_query RPAREN',
        'CREATE ANOMALY DETECTION MODEL identifier PREDICT result_columns',
        'CREATE ANOMALY DETECTION MODEL identifier PREDICT result_columns FROM identifier LPAREN raw_query RPAREN',
        'CREATE ANOMALY DETECTION MODEL identifier FROM identifier LPAREN raw_query RPAREN PREDICT result_columns',
        # TODO add IF_NOT_EXISTS elegantly (should be low level BNF expansion)
    )
    def create_anomaly_detection_model(self, p):

        query_str = None
        if hasattr(p, 'raw_query'):
            query_str = tokens_to_string(p.raw_query)

        if hasattr(p, 'identifier'):
            # single identifier field
            name = p.identifier
        else:
            name = p.identifier0

        return CreateAnomalyDetectionModel(
            name=name,
            targets=getattr(p, 'result_columns', None),
            integration_name=getattr(p, 'identifier1', None),
            query_str=query_str,
            if_not_exists=hasattr(p, 'IF_NOT_EXISTS')
        )

    @_('create_anomaly_detection_model USING kw_parameter_list')
    def create_anomaly_detection_model(self, p):
        p.create_anomaly_detection_model.using = p.kw_parameter_list
        return p.create_anomaly_detection_model

    # RETRAIN PREDICTOR

    @_('RETRAIN identifier',
       'RETRAIN identifier PREDICT result_columns',
       'RETRAIN identifier FROM LPAREN raw_query RPAREN',
       'RETRAIN identifier FROM LPAREN raw_query RPAREN PREDICT result_columns',
       'RETRAIN identifier FROM identifier LPAREN raw_query RPAREN',
       'RETRAIN identifier FROM identifier LPAREN raw_query RPAREN PREDICT result_columns',
       'RETRAIN MODEL identifier',
       'RETRAIN MODEL identifier PREDICT result_columns',
       'RETRAIN MODEL identifier FROM LPAREN raw_query RPAREN',
       'RETRAIN MODEL identifier FROM identifier LPAREN raw_query RPAREN',
       'RETRAIN MODEL identifier FROM LPAREN raw_query RPAREN PREDICT result_columns',
       'RETRAIN MODEL identifier FROM identifier LPAREN raw_query RPAREN PREDICT result_columns')
    def create_predictor(self, p):
        query_str = None
        if hasattr(p, 'raw_query'):
            query_str = tokens_to_string(p.raw_query)

        if hasattr(p, 'identifier'):
            # single identifier field
            name = p.identifier
        else:
            name = p.identifier0

        return RetrainPredictor(
            name=name,
            integration_name=getattr(p, 'identifier1', None),
            query_str=query_str,
            targets=getattr(p, 'result_columns', None)
        )

    @_('FINETUNE identifier FROM identifier LPAREN raw_query RPAREN',
       'FINETUNE identifier FROM LPAREN raw_query RPAREN',
       'FINETUNE MODEL identifier FROM identifier LPAREN raw_query RPAREN',
       'FINETUNE MODEL identifier FROM LPAREN raw_query RPAREN')
    def create_predictor(self, p):
        query_str = None
        if hasattr(p, 'raw_query'):
            query_str = tokens_to_string(p.raw_query)

        if hasattr(p, 'identifier'):
            # single identifier field
            name = p.identifier
        else:
            name = p.identifier0

        return FinetunePredictor(
            name=name,
            integration_name=getattr(p, 'identifier1', None),
            query_str=query_str,
        )

    @_('EVALUATE identifier FROM LPAREN raw_query RPAREN',
       'EVALUATE identifier FROM LPAREN raw_query RPAREN USING kw_parameter_list', )
    def evaluate(self, p):
        if hasattr(p, 'identifier'):
            # single identifier field
            name = p.identifier
        else:
            name = p.identifier0

        if hasattr(p, 'USING'):
            using = p.kw_parameter_list
        else:
            using = None

        return Evaluate(
            name=name,
            query_str=tokens_to_string(p.raw_query),
            using=using
        )

    # ------------

    # ML ENGINE
    # CREATE
    @_('CREATE ML_ENGINE if_not_exists_or_empty identifier FROM id USING kw_parameter_list',
       'CREATE ML_ENGINE if_not_exists_or_empty identifier FROM id')
    def create_integration(self, p):
        return CreateMLEngine(name=p.identifier,
                              handler=p.id,
                              params=getattr(p, 'kw_parameter_list', None),
                              if_not_exists=p.if_not_exists_or_empty)

    # DROP
    @_('DROP ML_ENGINE if_exists_or_empty identifier')
    def create_integration(self, p):
        return DropMLEngine(name=p.identifier, if_exists=p.if_exists_or_empty)

    # CREATE INTEGRATION
    @_('CREATE replace_or_empty database_engine',
       'CREATE replace_or_empty database_engine COMMA PARAMETERS EQUALS json',
       'CREATE replace_or_empty database_engine COMMA PARAMETERS json',
       'CREATE replace_or_empty database_engine PARAMETERS EQUALS json',
       'CREATE replace_or_empty database_engine PARAMETERS json',
       )
    def create_integration(self, p):
        is_replace = getattr(p, 'replace_or_empty', False)

        parameters = None
        if hasattr(p, 'json'):
            parameters = p.json

        return CreateDatabase(name=p.database_engine['identifier'],
                              engine=p.database_engine['engine'],
                              is_replace=is_replace,
                              parameters=parameters,
                              if_not_exists=p.database_engine['if_not_exists'])

    @_('DATABASE if_not_exists_or_empty identifier',
       'DATABASE if_not_exists_or_empty identifier ENGINE string',
       'DATABASE if_not_exists_or_empty identifier ENGINE EQUALS string',
       'DATABASE if_not_exists_or_empty identifier WITH ENGINE string',
       'DATABASE if_not_exists_or_empty identifier WITH ENGINE EQUALS string',
       'DATABASE if_not_exists_or_empty identifier USING ENGINE EQUALS string',
       'PROJECT if_not_exists_or_empty identifier')
    def database_engine(self, p):
        engine = None
        if hasattr(p, 'string'):
            engine = p.string
        return {'identifier':p.identifier, 'engine':engine, 'if_not_exists':p.if_not_exists_or_empty}

    # UNION / UNION ALL
    @_('select UNION select',
       'union UNION select')
    def union(self, p):
        return Union(left=p[0], right=p[2], unique=True)

    @_('select UNION ALL select',
       'union UNION ALL select',)
    def union(self, p):
        return Union(left=p[0], right=p[3], unique=False)

    # tableau
    @_('LPAREN select RPAREN')
    def select(self, p):
        return p.select

    # WITH
    @_('ctes select')
    def select(self, p):
        select = p.select
        select.cte = p.ctes
        return select

    @_('ctes COMMA identifier cte_columns_or_nothing AS LPAREN select RPAREN')
    def ctes(self, p):
        ctes = p.ctes
        ctes = ctes + [
            CommonTableExpression(
                name=p.identifier,
                columns=p.cte_columns_or_nothing,
                query=p.select)
        ]
        return ctes

    @_('WITH identifier cte_columns_or_nothing AS LPAREN select RPAREN')
    def ctes(self, p):
        return [
            CommonTableExpression(
                name=p.identifier,
                columns=p.cte_columns_or_nothing,
                query=p.select)
        ]

    @_('empty')
    def cte_columns_or_nothing(self, p):
        pass

    @_('LPAREN enumeration RPAREN')
    def cte_columns_or_nothing(self, p):
        return p.enumeration

    # SELECT
    @_('select FOR UPDATE')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'MODE')
        select.mode = 'FOR UPDATE'
        return select

    @_('select OFFSET constant')
    def select(self, p):
        select = p.select
        if select.offset is not None:
            raise ParsingException(f'OFFSET already specified for this query')
        ensure_select_keyword_order(select, 'OFFSET')
        if not isinstance(p.constant.value, int):
            raise ParsingException(f'OFFSET must be an integer value, got: {p.constant.value}')

        select.offset = p.constant
        return select

    @_('select LIMIT constant COMMA constant')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'LIMIT')
        if not isinstance(p.constant0.value, int) or not isinstance(p.constant1.value, int):
            raise ParsingException(f'LIMIT must have integer arguments, got: {p.constant0.value}, {p.constant1.value}')
        select.offset = p.constant0
        select.limit = p.constant1
        return select

    @_('select LIMIT constant')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'LIMIT')
        if not isinstance(p.constant.value, int):
            raise ParsingException(f'LIMIT must be an integer value, got: {p.constant.value}')
        select.limit = p.constant
        return select

    @_('select ORDER_BY ordering_terms')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'ORDER BY')
        select.order_by = p.ordering_terms
        return select

    @_('ordering_terms COMMA ordering_term')
    def ordering_terms(self, p):
        terms = p.ordering_terms
        terms.append(p.ordering_term)
        return terms

    @_('ordering_term')
    def ordering_terms(self, p):
        return [p.ordering_term]

    @_('ordering_term NULLS_FIRST')
    def ordering_term(self, p):
        p.ordering_term.nulls = p.NULLS_FIRST
        return p.ordering_term

    @_('ordering_term NULLS_LAST')
    def ordering_term(self, p):
        p.ordering_term.nulls = p.NULLS_LAST
        return p.ordering_term

    @_('identifier DESC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='DESC')

    @_('identifier ASC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='ASC')

    @_('identifier')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='default')

    @_('select USING kw_parameter_list')
    def select(self, p):
        p.select.using = p.kw_parameter_list
        return p.select

    @_('select HAVING expr')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'HAVING')
        having = p.expr
        if not isinstance(having, Operation):
            raise ParsingException(
                f"HAVING must contain an operation that evaluates to a boolean, got: {str(having)}")
        select.having = having
        return select

    @_('select GROUP_BY expr_list')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'GROUP BY')
        group_by = p.expr_list
        if not isinstance(group_by, list):
            group_by = [group_by]

        select.group_by = group_by
        return select

    @_('select WHERE expr')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'WHERE')
        where_expr = p.expr
        if not isinstance(where_expr, Operation):
            raise ParsingException(
                f"WHERE must contain an operation that evaluates to a boolean, got: {str(where_expr)}")
        select.where = where_expr
        return select

    @_('select FROM from_table_aliased',
       'select FROM join_tables_implicit',
       'select FROM join_tables')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'FROM')
        select.from_table = p[2]
        return select

    # --- join ---
    @_('from_table_aliased join_clause from_table_aliased',
       'join_tables join_clause from_table_aliased')
    def join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=p.join_clause)

    @_('from_table_aliased join_clause from_table_aliased ON expr',
       'join_tables join_clause from_table_aliased ON expr')
    def join_tables(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=p.join_clause,
                    condition=p.expr)

    @_('from_table_aliased COMMA from_table_aliased',
       'join_tables_implicit COMMA from_table_aliased')
    def join_tables_implicit(self, p):
        return Join(left=p[0],
                    right=p[2],
                    join_type=JoinType.INNER_JOIN,
                    implicit=True)

    @_('from_table AS identifier',
       'from_table identifier',
       'from_table AS dquote_string',
       'from_table dquote_string',
       'from_table')
    def from_table_aliased(self, p):
        entity = p.from_table
        if hasattr(p, 'identifier'):
            entity.alias = p.identifier
        if hasattr(p, 'dquote_string'):
            entity.alias = Identifier(p.dquote_string)
        return entity

    # native query
    @_('identifier LPAREN raw_query RPAREN')
    def from_table(self, p):
        query = NativeQuery(
            integration=p.identifier,
            query=tokens_to_string(p.raw_query)
        )
        return query

    @_('LPAREN query RPAREN')
    def from_table(self, p):
        query = p.query
        query.parentheses = True
        return query

    # keywords for table
    @_('PLUGINS',
       'ENGINES')
    def from_table(self, p):
        return Identifier.from_path_str(p[0])

    @_('identifier')
    def from_table(self, p):
        return p.identifier

    @_('parameter')
    def from_table(self, p):
        return p.parameter

    @_('JOIN',
       'LEFT JOIN',
       'RIGHT JOIN',
       'INNER JOIN',
       'FULL JOIN',
       'CROSS JOIN',
       'OUTER JOIN',
       'LEFT OUTER JOIN',
       'FULL OUTER JOIN',
       )
    def join_clause(self, p):
        return ' '.join([x for x in p])

    @_('SELECT DISTINCT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets, distinct=True)

    @_('SELECT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets)

    @_('result_columns COMMA result_column')
    def result_columns(self, p):
        p.result_columns.append(p.result_column)
        return p.result_columns

    @_('result_column')
    def result_columns(self, p):
        return [p.result_column]

    @_('result_column AS identifier',
       'result_column identifier',
       'result_column AS dquote_string',
       'result_column dquote_string',
       'result_column AS quote_string',
       'result_column quote_string')
    def result_column(self, p):
        col = p.result_column
        # if col.alias:
        #     raise ParsingException(f'Attempt to provide two aliases for {str(col)}')
        if hasattr(p, 'dquote_string'):
            alias = Identifier(p.dquote_string)
        elif hasattr(p, 'quote_string'):
            alias = Identifier(p.quote_string)
        else:
            alias = p.identifier
        col.alias = alias
        return col

    @_('LPAREN select RPAREN')
    def result_column(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('star')
    def result_column(self, p):
        return p.star

    @_('expr',
       'function',
       'window_function')
    def result_column(self, p):
        return p[0]

    # case
    @_('CASE case_conditions ELSE expr END')
    def case(self, p):
        return Case(rules=p.case_conditions, default=p.expr)

    @_('case_condition',
       'case_conditions case_condition')
    def case_conditions(self, p):
        arr = getattr(p, 'case_conditions', [])
        arr.append(p.case_condition)
        return arr

    @_('WHEN expr THEN expr')
    def case_condition(self, p):
        return [p.expr0, p.expr1]

    # Window function
    @_('function OVER LPAREN window RPAREN')
    def window_function(self, p):

        return WindowFunction(
            function=p.function,
            order_by=p.window.get('order_by'),
            partition=p.window.get('partition'),
        )

    @_('window PARTITION_BY expr_list')
    def window(self, p):
        window = p.window
        part_by = p.expr_list
        if not isinstance(part_by, list):
            part_by = [part_by]

        window['partition'] = part_by
        return window

    @_('window ORDER_BY ordering_terms')
    def window(self, p):
        window = p.window
        window['order_by'] = p.ordering_terms
        return window

    @_('empty')
    def window(self, p):
        return {}

    # OPERATIONS

    @_('LPAREN select RPAREN')
    def expr(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('LPAREN expr RPAREN')
    def expr(self, p):
        if isinstance(p.expr, ASTNode):
            p.expr.parentheses = True
        return p.expr

    @_('identifier LPAREN expr FROM expr RPAREN')
    def function(self, p):
        return Function(op=p[0].parts[0], args=[p.expr0], from_arg=p.expr1)

    @_('DATABASE LPAREN RPAREN')
    def function(self, p):
        return Function(op=p.DATABASE, args=[])

    @_('identifier LPAREN DISTINCT expr_list RPAREN')
    def function(self, p):
        return Function(op=p[0].parts[0], distinct=True, args=p.expr_list)

    @_(
       'function_name LPAREN expr_list_or_nothing RPAREN',
       'identifier LPAREN expr_list_or_nothing RPAREN',
       'identifier LPAREN star RPAREN')
    def function(self, p):
        if hasattr(p, 'star'):
            args = [p.star]
        else:
            args = p.expr_list_or_nothing
        if not args:
            args = []
        namespace = None
        if hasattr(p, 'identifier'):
            if len(p.identifier.parts) > 1:
                namespace = p.identifier.parts[0]
            name = p.identifier.parts[-1]
        else:
            name = p.function_name
        return Function(op=name, args=args, namespace=namespace)

    @_('INTERVAL string')
    def expr(self, p):
        return Interval(p.string)

    # arguments are optional in functions, so that things like `select database()` are possible
    @_('expr BETWEEN expr AND expr')
    def expr(self, p):
        return BetweenOperation(args=(p.expr0, p.expr1, p.expr2))

    @_('expr_list')
    def expr_list_or_nothing(self, p):
        return p.expr_list

    @_('empty')
    def expr_list_or_nothing(self, p):
        pass

    @_('CAST LPAREN expr AS id LPAREN integer RPAREN RPAREN')
    def expr(self, p):
        return TypeCast(arg=p.expr, type_name=str(p.id), length=p.integer)

    @_('CAST LPAREN expr AS id RPAREN')
    def expr(self, p):
        return TypeCast(arg=p.expr, type_name=str(p.id))

    @_('CONVERT LPAREN expr COMMA id RPAREN',
       'CONVERT LPAREN expr USING id RPAREN')
    def expr(self, p):
        return TypeCast(arg=p.expr, type_name=str(p.id))

    @_('enumeration')
    def expr_list(self, p):
        return p.enumeration

    @_('expr')
    def expr_list(self, p):
        return [p.expr]

    @_('LPAREN enumeration RPAREN')
    def expr(self, p):
        tup = Tuple(items=p.enumeration)
        return tup

    @_('STAR')
    def star(self, p):
        return Star()

    @_('expr NOT IN expr')
    def expr(self, p):
        op = p[1] + ' ' + p[2]
        return BinaryOperation(op=op, args=(p.expr0, p.expr1))

    @_('expr PLUS expr',
       'expr MINUS expr',
       'expr STAR expr',
       'expr DIVIDE expr',
       'expr MODULO expr',
       'expr EQUALS expr',
       'expr NEQUALS expr',
       'expr GEQ expr',
       'expr GREATER expr',
       'expr GEQ LAST',
       'expr GREATER LAST',
       'expr LEQ expr',
       'expr LESS expr',
       'expr AND expr',
       'expr OR expr',
       'expr IS_NOT expr',
       'expr NOT expr',
       'expr IS expr',
       'expr LIKE expr',
       'expr NOT_LIKE expr',
       'expr CONCAT expr',
       'expr JSON_GET constant',
       'expr JSON_GET_STR constant',
       'expr IN expr')
    def expr(self, p):
        if hasattr(p, 'LAST'):
            arg1 = Last()
        else:
            arg1 = p[2]
        return BinaryOperation(op=p[1], args=(p[0], arg1))

    @_('MINUS expr %prec UMINUS',
       'NOT expr %prec UNOT', )
    def expr(self, p):
        return UnaryOperation(op=p[0], args=(p.expr,))

    @_('MINUS constant %prec UMINUS')
    def constant(self, p):
        return Constant(-p.constant.value)

    # update fields list
    @_('update_parameter',
       'update_parameter_list COMMA update_parameter')
    def update_parameter_list(self, p):
        params = getattr(p, 'update_parameter_list', {})
        params.update(p.update_parameter)
        return params

    @_('id EQUALS expr')
    def update_parameter(self, p):
        return {p.id:p.expr}

    # EXPRESSIONS

    @_('enumeration COMMA expr')
    def enumeration(self, p):
        return p.enumeration + [p.expr]

    @_('expr COMMA expr')
    def enumeration(self, p):
        return [p.expr0, p.expr1]

    @_('identifier',
       'parameter',
       'constant',
       'latest',
       'case',
       'function')
    def expr(self, p):
        return p[0]

    @_('LATEST')
    def latest(self, p):
        return Latest()

    @_('NULL')
    def constant(self, p):
        return NullConstant()

    @_('TRUE')
    def constant(self, p):
        return Constant(value=True)

    @_('FALSE')
    def constant(self, p):
        return Constant(value=False)

    @_('integer')
    def constant(self, p):
        return Constant(value=int(p.integer))

    @_('float')
    def constant(self, p):
        return Constant(value=float(p.float))

    @_('string')
    def constant(self, p):
        return Constant(value=str(p[0]))

    # param list

    @_('id LPAREN kw_parameter_list RPAREN')
    def object(self, p):
        return Object(type=p.id, params=p.kw_parameter_list)

    @_('kw_parameter',
       'kw_parameter_list COMMA kw_parameter')
    def kw_parameter_list(self, p):
        params = getattr(p, 'kw_parameter_list', {})
        params.update(p.kw_parameter)
        return params

    @_('identifier EQUALS object',
       'identifier EQUALS json_value',
       'identifier EQUALS identifier')
    def kw_parameter(self, p):
        key = getattr(p, 'identifier', None) or getattr(p, 'identifier0', None)
        assert key is not None
        key = '.'.join(key.parts)
        return {key:p[2]}

    # json

    @_('LBRACE json_element_list RBRACE',
       'LBRACE RBRACE')
    def json(self, p):
        params = getattr(p, 'json_element_list', {})
        return params

    @_('json_element',
       'json_element_list COMMA json_element')
    def json_element_list(self, p):
        params = getattr(p, 'json_element_list', {})
        params.update(p.json_element)
        return params

    @_('string COLON json_value')
    def json_element(self, p):
        return {p.string:p.json_value}

    # json_array

    @_('LBRACKET json_array_list RBRACKET',
       'LBRACKET RBRACKET')
    def json_array(self, p):
        arr = getattr(p, 'json_array_list', [])
        return arr

    @_('json_value',
       'json_array_list COMMA json_value')
    def json_array_list(self, p):
        arr = getattr(p, 'json_array_list', [])
        arr.append(p.json_value)
        return arr

    @_('float',
       'string',
       'integer',
       'NULL',
       'TRUE',
       'FALSE',
       'json_array',
       'json')
    def json_value(self, p):

        if hasattr(p, 'NULL'):
            return None
        elif hasattr(p, 'TRUE'):
            return True
        elif hasattr(p, 'FALSE'):
            return False
        return p[0]

    @_('identifier DOT identifier',
       'identifier DOT integer',
       'identifier DOT star')
    def identifier(self, p):
        node = p[0]
        if isinstance(p[2], Star):
            node.parts.append(p[2])
        elif isinstance(p[2], int):
            node.parts.append(str(p[2]))
        else:
            node.parts += p[2].parts
        return node

    @_('id')
    def identifier(self, p):
        value = p[0]
        return Identifier.from_path_str(value)

    @_('quote_string',
       'dquote_string')
    def string(self, p):
        return p[0]

    @_('PARAMETER')
    def parameter(self, p):
        return Parameter(value=p.PARAMETER)

    @_('id',
       'FULL',
       'RIGHT',
       'LEFT')
    def function_name(self, p):
        return p[0]

    # convert to types
    @_('ID',
       'BEGIN',
       'CAST',
       'CHANNEL',
       'CHARSET',
       'CODE',
       'COLLATION',
       'COLUMNS',
       'COMMIT',
       'COMMITTED',
       'DATASET',
       'DATASETS',
       'DATABASE',
       'DATABASES',
       'DATASOURCE',
       'DATASOURCES',
       'ENGINE',
       'ENGINES',
       'EXTENDED',
       'FIELDS',
       'GLOBAL',
       'HORIZON',
       'HOSTS',
       'INDEXES',
       'INDEX',
       'INTEGRATION',
       'INTEGRATIONS',
       'INTERVAL',
       'ISOLATION',
       'KEYS',
       'LATEST',
       'LAST',
       'LEVEL',
       'LOGS',
       'MASTER',
       'MUTEX',
       'OFFSET',
       'ONLY',
       'OPEN',
       'PARAMETERS',
       'PERSIST',
       'PLUGINS',
       'PREDICT',
       'PREDICTOR',
       'PREDICTORS',
       'PRIVILEGES',
       'PROCESSLIST',
       'PROFILES',
       'PUBLICATION',
       'PUBLICATIONS',
       'REPEATABLE',
       'REPLACE',
       'REPLICA',
       'REPLICAS',
       'RETRAIN',
       'ROLLBACK',
       'SERIALIZABLE',
       'SESSION',
       'SCHEMA',
       'SLAVE',
       'START',
       'STATUS',
       'STORAGE',
       'STREAM',
       'STREAMS',
       'TABLES',
       'TABLE',
       'TRAIN',
       'TRANSACTION',
       'TRIGGERS',
       'UNCOMMITTED',
       'VARIABLES',
       'VIEW',
       'VIEWS',
       'WARNINGS',
       'MODEL',
       'MODELS',
       'AGENT',
       'SCHEMAS',
       'FUNCTION',
       'charset',
       'PROCEDURE',
       'ML_ENGINES',
       'HANDLERS',
       'BINARY',
       'KNOWLEDGE_BASES',
       'ALL',
       'CREATE',
       )
    def id(self, p):
        return p[0]

    @_('FLOAT')
    def float(self, p):
        return float(p[0])

    @_('INTEGER')
    def integer(self, p):
        return int(p[0])

    @_('QUOTE_STRING')
    def quote_string(self, p):
        return p[0].strip('\'')

    @_('DQUOTE_STRING')
    def dquote_string(self, p):
        return p[0].strip('\"')

    # for raw query

    @_('LPAREN raw_query RPAREN')
    def raw_query(self, p):
        return [p._slice[0]] + p[1] + [p._slice[2]]

    @_('raw_query LPAREN RPAREN')
    def raw_query(self, p):
        return p[0] + [p._slice[1], p._slice[2]]

    @_('raw_query raw_query')
    def raw_query(self, p):
        return p[0] + p[1]

    @_('variable')
    def table_or_subquery(self, p):
        return p.variable

    @_('variable')
    def expr(self, p):
        return p.variable

    @_('SYSTEM_VARIABLE')
    def variable(self, p):
        return Variable(value=p.SYSTEM_VARIABLE, is_system_var=True)

    @_('VARIABLE')
    def variable(self, p):
        return Variable(value=p.VARIABLE)

    @_(
        'OR REPLACE',
        'empty'
    )
    def replace_or_empty(self, p):
        if hasattr(p, 'REPLACE'):
            return True
        return False

    @_(
        'IF_NOT_EXISTS',
        'empty'
    )
    def if_not_exists_or_empty(self, p):
        if hasattr(p, 'IF_NOT_EXISTS'):
            return True
        return False

    @_(
        'IF_EXISTS',
        'empty'
    )
    def if_exists_or_empty(self, p):
        if hasattr(p, 'IF_EXISTS'):
            return True
        return False

    @_(*all_tokens_list)
    def raw_query(self, p):
        return p._slice

    @_('')
    def empty(self, p):
        pass

    def error(self, p, expected_tokens=None):

        if not hasattr(self, 'used_tokens'):
            # failback mode if user has another sly version module installed
            if p:
                raise ParsingException(f"Syntax error at token {p.type}: \"{p.value}\"")
            else:
                raise ParsingException("Syntax error at EOF")

        # save error info for future usage
        self.error_info = dict(
            tokens=self.used_tokens.copy() + list(self.tokens),
            bad_token=p,
            expected_tokens=expected_tokens
        )
        # don't raise exception
        return
