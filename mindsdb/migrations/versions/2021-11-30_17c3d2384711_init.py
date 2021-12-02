from mindsdb.interfaces.storage.db import Base

from alembic.autogenerate import produce_migrations, render, api
from alembic import context

# required for code execution
import mindsdb.interfaces.storage.db
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '17c3d2384711'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    '''
       First migration.
       Generates a migration script by difference between model and database and executes it
    '''


    target_metadata = Base.metadata

    mc = context.get_context()

    migration_script = produce_migrations(mc, target_metadata)

    autogen_context = api.AutogenContext(
        mc, autogenerate=True
    )

    # Seems to be the only way to apply changes to the database
    template_args = {}
    render._render_python_into_templatevars(
        autogen_context, migration_script, template_args
    )

    code = template_args['upgrades']
    code = code.replace('\n    ', '\n')
    print('\nPerforming database changes:')
    print(code)
    exec(code)


def downgrade():

    # We don't know state to downgrade
    raise NotImplemented()
