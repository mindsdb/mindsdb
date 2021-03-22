import json
import datetime
import shutil
import os
import pickle

import pandas as pd

from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface
from mindsdb_datasources import FileDS, ClickhouseDS, MariaDS, MySqlDS, PostgresDS, MSSQLDS, MongoDS, SnowflakeDS
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.db import session, Datasource, AITable
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.utilities.log import log


class AITable_store():
    def __init__(self):
        self.config = Config()

        self.fs_store = FsSotre()
        self.company_id = os.environ.get('MINDSDB_COMPANY_ID', None)
        self.dir = self.config.paths['datasources']
        self.mindsdb_native = NativeInterface()

    def is_ai_table(self, name):
        record = self.get_ai_table(name.lower())
        return record is not None

    def get_ai_table(self, name):
        ''' get particular ai table
        '''
        aitable_record = session.query(AITable).filter_by(company_id=self.company_id, name=name.lower()).first()
        return aitable_record

    def get_ai_tables(self):
        ''' get list of ai tables
        '''
        aitable_records = [x.__dict__ for x in session.query(AITable).filter_by(company_id=self.company_id)]
        return aitable_records

    def add(self, name, integration_name, integration_query, query_fields, predictor_name, predictor_fields):
        ai_table_record = AITable(
            name=name.lower(),
            integration_name=integration_name,
            integration_query=integration_query,
            query_fields=query_fields,
            predictor_name=predictor_name,
            predictor_columns=predictor_fields
        )
        session.add(ai_table_record)
        session.commit()

    def query(self, name, where=None):
        ''' query to ai table
        '''
        pass
