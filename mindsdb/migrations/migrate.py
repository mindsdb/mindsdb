from pathlib import Path

from alembic.command import upgrade, autogen
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.script.revision import ResolutionError
from alembic.migration import MigrationContext

import mindsdb.interfaces.storage.db as db


def migrate_to_head():
    '''
        Trying to update database to head revision.
        If alembic unable to recognize current revision (In case when database version is newer than backend)
         then do nothing.
    '''

    config_file = Path(__file__).parent / 'alembic.ini'
    config = Config(config_file)

    # mindsdb can runs not from project directory
    script_location_abc = config_file.parent / config.get_main_option('script_location')
    config.set_main_option('script_location', str(script_location_abc))

    script = ScriptDirectory.from_config(config)
    with db.engine.begin() as conn:
        mc = MigrationContext.configure(conn)
        cur_revision = mc.get_current_revision()

        try:
            script.revision_map.get_revision(cur_revision)

        except ResolutionError:
            print('!!! Database version higher than application !!!')
            return

    upgrade(config=config, revision='head')
