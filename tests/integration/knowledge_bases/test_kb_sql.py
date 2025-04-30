import json
import time
import datetime as dt

import mindsdb_sdk
from threading import Thread

import pandas as pd


class KBTestBase:

    def __init__(self, vectordb=None, emb_model=None, mindsdb_server=None):
        self.vectordb = vectordb
        self.vectordb_engine = vectordb['engine']
        self.emb_model = emb_model
        if mindsdb_server is None:
            # use localhost
            mindsdb_server = {
                'url': 'http://127.0.0.1:47334/'
            }

        self.con = mindsdb_sdk.connect(**mindsdb_server)

        self.prepare()

    def create_vector_db(self, connection_args):
        engine = connection_args.pop('engine')

        name = f'test_vectordb_{engine}'

        try:
            self.con.databases.drop(name)
        except RuntimeError as e:
            if 'Database does not exists' not in str(e):
                raise e
        # if engine == 'pgvector':
        #     connection_args = {}
        # elif engine == 'chromadb':
        #     connection_args = {}

        self.con.databases.create(name, engine=engine, connection_args=connection_args)
        return name

    def create_db(self):
        name = 'example_db'

        try:
            self.con.databases.get(name)
            return name

        except AttributeError:
            pass

        self.con.databases.create(name, engine='postgres', connection_args={
            "user": "demo_user",
            "password": "demo_password",
            "host": "samples.mindsdb.com",
            "port": "5432",
            "database": "demo",
            "schema": "demo_data"
        })
        return name

    def create_emb_model(self, params):
        engine = params.pop('engine')

        name = f'test_emb_model_{engine}'
        try:
            self.con.models.drop(name)
        except Exception:
            ...

        if engine == 'openai':
            model = self.con.models.create(
                name, predict='embedding', engine=engine,
                options=params,
            )
        elif engine == 'langchain_embedding':
            model = self.con.models.create(
                name, predict='embedding', engine=engine,
                options=params,
            )
        model.wait_complete()
        return model.name

    def run_sql(self, sql):
        print('>>>', sql)
        resp = self.con.query(sql).fetch()
        print('--- response ---')
        print(resp)
        return resp

    def prepare(self):
        # drop existed kb:
        for name in ('kb_crm', 'kb_crm_part', 'kb_home_rentals'):
            self.run_sql(f'drop knowledge base if exists {name}')

        # create vector_db
        self.vectordb_name = self.create_vector_db(self.vectordb)

        # create embedding model
        self.emb_model = self.create_emb_model(self.emb_model)

        # connect database
        self.db_name = self.create_db()

    def create_kb(self, name, params=None, with_model=True):
        model_str = ''
        if with_model:
            model_str = f'model = {self.con.models.get(self.emb_model)}, '

        self.run_sql(f'drop knowledge base if exists {name}')

        param_str = ''
        if params:
            param_items = []
            for k, v in params.items():
                param_items.append(f'{k}={json.dumps(v)}')
            param_str = ',' + ','.join(param_items)

        self.run_sql(f'''
            create knowledge base {name}
            using {model_str}
               storage = {self.vectordb_name}.tbl_{name}
               {param_str}
        ''')

        # clean
        if len(self.run_sql(f'select * from {name}')) > 0:
            self.run_sql(f'delete from {name} where id in (select id from {name})')

        ret = self.run_sql(f'describe knowledge base {name}')
        assert len(ret) == 1


class KBTest(KBTestBase):
    def test_base_syntax(self):
        # TODO, what is "Confirm data persistence and integrity in vector store"

        '''
            Verify successful setup and operation
            Test creation of Knowledge Bases
        '''
        self.create_kb('kb_crm')

        # -------------- insert --------
        print('insert from table')
        count_rows = 10  # content too small to be chunked
        self.run_sql(f"""
            insert into kb_crm
            select pk id, message_body content from example_db.crm_demo
            order by pk
            limit {count_rows}
        """)

        kb_columns = ['id', 'chunk_content', 'metadata', 'distance', 'relevance']

        print('insert from values')
        for i in range(2):
            # do it twice second time it will be updated
            self.run_sql("""
                insert into kb_crm (id, content) values
                (1000, 'Help'), (1001, 'Thank you')
            """)
        count_rows += 2

        # ------------  checking columns  ------------
        # Simplified SQL Syntax

        print('Select all without conditions')
        ret = self.run_sql("select * from kb_crm")
        assert len(ret) == count_rows
        for column in kb_columns:
            assert column in ret.columns, f'Column {column} does not exist in response'

        print('Select one column without conditions')
        for column in kb_columns:
            ret = self.run_sql(f"select {column} from kb_crm")
            assert len(ret) == count_rows
            assert list(ret.columns) == [column], f"Response don''t have column {column}"

        # ---------- selecting options --------

        print('Limit')
        ret = self.run_sql("select id, chunk_content from kb_crm limit 4")
        assert len(ret) == 4

        print('Limit with content')
        ret = self.run_sql("select id, chunk_content, distance from kb_crm where content = 'help' limit 4")
        assert len(ret) == 4
        assert ret['id'][0] == '1000'  # id is string

        print('filter by id')
        ret = self.run_sql("select id, chunk_content from kb_crm where id = '1001'")
        assert len(ret) == 1
        assert ret['chunk_content'][0] == 'Thank you'

        ret = self.run_sql("select id, chunk_content from kb_crm where id != '1000' limit 4")
        assert len(ret) == 4
        assert '1000' not in ret['id']

        # in, not in
        ret = self.run_sql("select id, chunk_content from kb_crm where id in ('1001', '1000')")
        assert len(ret) == 2
        assert set(ret['id']) == {'1000', '1001'}

        ret = self.run_sql("select id, chunk_content from kb_crm where id not in ('1001', '1000') limit 4")
        assert len(ret) == 4
        assert '1000' not in list(ret['id'])

        if self.vectordb_engine != 'chromadb':
            # some operators don't work with chromadb

            # like / not like
            ret = self.run_sql("select id, chunk_content from kb_crm where id like '100%'")
            assert len(ret) == 2
            assert '1001' in list(ret['id'])

            ret = self.run_sql("select id, chunk_content from kb_crm where id not like '100%' limit 4")
            assert len(ret) == 4
            assert '1001' not in list(ret['id'])

        # TODO filtering combination with content

        # ------------------- join with table -------------
        ret = self.run_sql("""
            select k.chunk_content, t.message_body, k.id, t.pk
            from kb_crm k
            join example_db.crm_demo t on t.pk = k.id
            where k.content = 'Help' and k.id not in ('1001', '1000')
            limit 4
        """)

        row = ret.iloc[0]
        assert row['chunk_content'] == row['message_body']
        assert row['id'] == str(row['pk'])

        # -----------------  modify data ---------------
        '''
        Test adding new vectors/records to the knowledge base
        Test updating existing records
        Test deleting records/vectors by ID
        '''

        # delete
        self.run_sql("delete from kb_crm where id = '1'")
        ret = self.run_sql("select * from kb_crm where id = '1'")
        assert len(ret) == 0

        self.run_sql("delete from kb_crm where id in ('1001', '2')")
        ret = self.run_sql("select * from kb_crm where id in ('1001', '2')")
        assert len(ret) == 0

        self.run_sql("delete from kb_crm where id in ('1001', '1')")

        # insert
        self.run_sql("""
            insert into kb_crm (id, content) values (2000, 'OK')
        """)
        ret = self.run_sql("select * from kb_crm where id = '2000'")
        assert len(ret) == 1
        assert ret['chunk_content'][0] == 'OK'
        chunk_id = ret['chunk_id'][0]

        # update
        self.run_sql(f"update kb_crm set content = 'FINE' where chunk_id = '{chunk_id}'")
        ret = self.run_sql("select * from kb_crm where id = '2000'")
        assert len(ret) == 1
        assert ret['chunk_content'][0] == 'FINE'

        # TODO update by id don't work
        #   should it update all chunks?

        # Test deletion of Knowledge Bases
        self.run_sql('drop knowledge base kb_crm')

        ret = self.run_sql('describe knowledge base kb_crm')
        assert len(ret) == 0

    def check_soft_delete(self):
        """
        Test the soft delete and recovery functionality for KBs
        Test soft delete and recovery functionality for individual records
        TODO NOT IMPLEMENTED
        """

    def test_ingestion(self):
        """
        Test scheduled data ingestion
        Partition
        Test querying status of Knowledge Base , DESCRIBE
        """

        def to_date(s):
            return dt.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')

        def load_kb(batch_size):
            self.run_sql(f"""
                insert into kb_crm_part
                select pk id, message_body content from example_db.crm_demo
                using batch_size={batch_size}, track_column=id
            """)

        test_set = [
            {'batch_size': 50},
            {'batch_size': 100},
        ]

        results = []
        for item in test_set:
            # Create KB and start load in thread
            self.create_kb('kb_crm_part')

            print('start loading')
            thread = Thread(target=load_kb, kwargs=item)
            thread.start()

            time.sleep(3)
            try:
                for i in range(100):  # 300 sec min max
                    time.sleep(3)

                    ret = self.run_sql('describe knowledge base kb_crm_part')
                    record = ret.iloc[0]

                    if record['QUERY_ID'] is None:
                        raise RuntimeError('Query is not partitioned')

                    if record['INSERT_FINISHED_AT'] is not None:
                        print('loading completed')
                        item['seconds'] = (to_date(record['INSERT_FINISHED_AT']) - to_date(record['INSERT_STARTED_AT'])).seconds
                        results.append(item)
                        break

                    if record['ERROR'] is not None:
                        raise RuntimeError(record['ERROR'])
            finally:
                thread.join()
        print('========= Tests summary ==========')
        print(pd.DataFrame(results))

    def test_metadata(self):
        self.create_kb('kb_crm', params={
            'metadata_columns': ['status', 'category'],
            'content_columns': ['message_body'],
            'id_column': 'id',
        })

        self.run_sql("""
            INSERT INTO kb_crm (
                SELECT * FROM example_db.crm_demo
            );
        """)

        # -- Metadata search
        ret = self.run_sql("""
            SELECT *
            FROM kb_crm
            WHERE category = "Battery";
        """)
        assert set(ret.metadata.apply(lambda x: x.get('category'))) == {'Battery'}

        ret = self.run_sql("""
            SELECT *
            FROM kb_crm
            WHERE status = "solving" AND category = "Battery"
        """)
        assert set(ret.metadata.apply(lambda x: x.get('category'))) == {'Battery'}
        assert set(ret.metadata.apply(lambda x: x.get('status'))) == {'solving'}

        # -- Content + metadata search
        ret = self.run_sql("""
            SELECT *
            FROM kb_crm
            WHERE status = "solving" AND content = "noise";
        """)
        assert set(ret.metadata.apply(lambda x: x.get('status'))) == {'solving'}
        assert 'noise' in ret.chunk_content[0]

        # -- Content + metadata search with limit
        ret = self.run_sql("""
            SELECT *
            FROM kb_crm
            WHERE status = "solving" AND content = "noise"
            LIMIT 5;
        """)
        assert set(ret.metadata.apply(lambda x: x.get('status'))) == {'solving'}
        assert 'noise' in ret.chunk_content[0]
        assert len(ret) == 5

        # -- Content + metadata search with limit and re-ranking threshold
        # TODO chroma shows max relevance = 0.69, but postgres = 0.81
        ret = self.run_sql("""
            SELECT *
            FROM kb_crm
            WHERE status = "solving" AND content = "noise" AND relevance_threshold=0.65
        """)
        assert set(ret.metadata.apply(lambda x: x.get('status'))) == {'solving'}
        assert 'noise' in ret.chunk_content[0]  # first line contents word
        assert len(ret[ret.relevance < 0.65]) == 0

    def test_dict_as_model(self, openai_api_key, reranking_model=None, embedding_model=None):

        def _check_kb(kb_params):
            self.create_kb('kb_crm', params=kb_params, with_model=False)

            self.run_sql("""
                INSERT INTO kb_crm (
                    SELECT * FROM example_db.crm_demo
                );
            """)

            threshold = 0.5
            ret = self.run_sql(f"""
                SELECT *
                FROM kb_crm
                WHERE status = "solving" AND content = "noise" AND relevance_threshold={threshold}
            """)
            assert set(ret.metadata.apply(lambda x: x.get('status'))) == {'solving'}
            for item in ret.chunk_content:
                assert 'noise' in item  # all lines line contents word

            assert len(ret[ret.relevance < threshold]) == 0

        if reranking_model is None:
            reranking_model = {
                "provider": "openai",
                "model_name": "gpt-4",
                "api_key": openai_api_key
            }

        if embedding_model is None:
            embedding_model = {
                "provider": "openai",
                "model_name": "text-embedding-ada-002",
                "api_key": openai_api_key
            }

        # prepare KB
        kb_params = {
            'embedding_model': embedding_model,
            'reranking_model': reranking_model,
            'metadata_columns': ['status', 'category'],
            'content_columns': ['message_body'],
            'id_column': 'id',
        }

        _check_kb(kb_params)

        # check with model name
        kb_params['embedding_model'] = self.emb_model

        _check_kb(kb_params)
