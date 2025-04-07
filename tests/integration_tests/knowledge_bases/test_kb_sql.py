import time
import datetime as dt

import mindsdb_sdk
from threading import Thread

import pandas as pd


class KBTest:

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
        except RuntimeError:
            pass
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
            self.con.models.get(name)
            return name
        except AttributeError:
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
        # create vector_db
        self.vectordb_name = self.create_vector_db(self.vectordb)

        # create embedding model
        self.emb_model = self.create_emb_model(self.emb_model)

        # connect database
        self.db_name = self.create_db()

    def create_kb(self, name):
        self.run_sql(f'drop knowledge base if exists {name}')

        self.run_sql(f'''
                    create knowledge base {name}
                    using  model={self.emb_model},
                    storage={self.vectordb_name}.crm_table
                ''')

        # clean
        try:
            self.run_sql(f'delete from kb_crm where id in (select id from {name})')
        except Exception:
            ...

        ret = self.run_sql(f'describe knowledge base {name}')
        assert len(ret) == 1

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
        ret = self.run_sql("select id, chunk_content from kb_crm where content = 'help' limit 4")
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

        def load_kb(batch_size, threads):
            self.run_sql(f"""
                insert into kb_crm_part
                select pk id, message_body content from example_db.crm_demo
                using batch_size={batch_size}, track_column=id, threads={threads}
            """)

        test_set = [
            {'batch_size': 10, 'threads': 'false'},
            {'batch_size': 10, 'threads': 5},
            {'batch_size': 50, 'threads': 'false'},
            {'batch_size': 50, 'threads': 5},
        ]

        results = []
        for item in test_set:
            # Create KB and start load in thread
            self.create_kb('kb_crm_part')

            print('start loading')
            thread = Thread(target=load_kb, kwargs=item)
            thread.start()

            try:
                while True:
                    time.sleep(1)

                    ret = self.run_sql('describe knowledge base kb_crm_part')
                    record = ret.iloc[0]

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

    def test_relevance(self):
        """
        SELECT id, relevance FROM my_kb WHERE content = 'a novel about epic inter planetary intelligence' LIMIT 10;

        SELECT id, chunk_id, chunk_content,chunk_relevance FROM my_kb WHERE content = 'a novel about epic inter planetary intelligence' LIMIT 10;

        """
        # TODO
