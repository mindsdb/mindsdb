import os
import time
import pytest
from pathlib import Path

import mindsdb.interfaces.storage.db as db
from mindsdb.migrations import migrate
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.fs import create_dirs_recursive
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.integrations.libs.const import PREDICTOR_STATUS
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.utilities.test_utils import PG_HANDLER_NAME, PG_CONNECTION_DATA
from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler

USE_PERSISTENT_STORAGE = bool(int(os.getenv('USE_PERSISTENT_STORAGE') or "0"))
TEST_CONFIG = Path(os.path.dirname(os.path.realpath(__file__))).joinpath('../integration_tests/flows/config/config.json').resolve()
TEMP_DIR = Path(__file__).parent.parent.absolute().joinpath(
    f'temp/test_storage_{int(time.time() * 1000)}/' if not USE_PERSISTENT_STORAGE else 'temp/test_storage/'
).resolve()
TEMP_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = TEMP_DIR.joinpath('config.json')
os.environ['MINDSDB_STORAGE_DIR'] = os.environ.get('MINDSDB_STORAGE_DIR', str(TEMP_DIR))
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'
os.environ['MINDSDB_CONFIG_PATH'] = str(TEST_CONFIG)

OPEN_AI_API_KEY = os.environ.get("OPEN_AI_API_KEY")
os.environ["OPENAI_API_KEY"] = OPEN_AI_API_KEY


@pytest.fixture(scope="module")
def handler():
    ctx.set_default()
    mindsdb_config = Config()
    create_dirs_recursive(mindsdb_config['paths'])
    db.init()
    migrate.migrate_to_head()

    integration_record = db.Integration(
        name='openai',
        data={},
        engine='openai',
        company_id=None
    )
    db.session.add(integration_record)
    db.session.commit()

    integration_record = db.Integration(
        name=PG_HANDLER_NAME,
        data=PG_CONNECTION_DATA,
        engine='postgres',
        company_id=None
    )
    db.session.add(integration_record)
    db.session.commit()

    handler = BaseMLEngineExec(
        'openai',
        handler_controller=IntegrationController(),
        fs_store=FsStore(),
        handler_class=OpenAIHandler
    )
    return handler


class TestOpenAIHandler:
    def test_01_qa_no_context_flow(self, handler):
        predictor_record = handler.learn(
            model_name='test_openai_qa_no_context',
            project_name='mindsdb',
            join_learn_process=True,
            problem_definition={
                'target': 'answer',
                'using': {
                    'openai_api_key': OPEN_AI_API_KEY,
                    'question_column': 'question'
                }
            }
        )
        assert isinstance(predictor_record, db.Predictor)
        assert predictor_record.status == PREDICTOR_STATUS.GENERATING
