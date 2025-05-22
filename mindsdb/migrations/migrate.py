from pathlib import Path

from alembic.command import upgrade, autogen    # noqa
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.script.revision import ResolutionError
from alembic.operations import Operations
from alembic.migration import MigrationContext
from alembic import util

import mindsdb.interfaces.storage.db as db
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# This is a migration that is like a 'base version'. Applying only this
# migration to a fresh DB is equivalent to applying all previous migrations.
current_checkpoint = '9f150e4f9a05'


def apply_checkpoint_migration(script) -> None:
    """Apply the checkpoint migration to the database.
    """
    with db.engine.begin() as connection:
        context = MigrationContext.configure(
            connection,
            opts={
                'as_sql': False,
                'starting_rev': None,  # ignore current version
                'destination_rev': current_checkpoint,
            }
        )
        revision = script.get_revision(current_checkpoint)
        if not revision:
            raise util.CommandError(f"Migration {current_checkpoint} not found.")

        op = Operations(context)
        revision.module.upgrade(op)
        context.stamp(script, current_checkpoint)
        connection.commit()


def get_current_revision() -> str | None:
    """ Get the current revision of the database.

    Returns:
        str | None: The current revision of the database.
    """
    with db.engine.begin() as conn:
        mc = MigrationContext.configure(conn)
        return mc.get_current_revision()


def migrate_to_head():
    """ Trying to update database to head revision.
        If alembic unable to recognize current revision (In case when database version is newer than backend)
        then do nothing.
    """

    config_file = Path(__file__).parent / 'alembic.ini'
    config = Config(config_file)

    # mindsdb can runs not from project directory
    script_location_abc = config_file.parent / config.get_main_option('script_location')
    config.set_main_option('script_location', str(script_location_abc))

    script = ScriptDirectory.from_config(config)
    cur_revision = get_current_revision()
    if cur_revision is None:
        apply_checkpoint_migration(script)
        cur_revision = get_current_revision()

    try:
        script.revision_map.get_revision(cur_revision)
    except ResolutionError:
        raise Exception("Database version higher than application.")

    head_rev = script.get_current_head()
    if cur_revision == head_rev:
        logger.debug("The database is in its current state, no updates are required.")
        return

    logger.info("Migrations are available. Applying updates to the database.")
    upgrade(config=config, revision='head')


if __name__ == "__main__":
    # have to import this because
    # all env initialization happens here
    db.init()
    migrate_to_head()
