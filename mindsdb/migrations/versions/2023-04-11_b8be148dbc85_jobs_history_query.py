"""jobs_history_query

Revision ID: b8be148dbc85
Revises: ef04cdbe51ed
Create Date: 2023-04-11 17:35:16.273293

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

# revision identifiers, used by Alembic.
revision = 'b8be148dbc85'
down_revision = 'ef04cdbe51ed'
branch_labels = None
depends_on = None


def upgrade():

    with op.batch_alter_table('jobs_history', schema=None) as batch_op:
        batch_op.add_column(sa.Column('query_str', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('updated_at', sa.DateTime(), nullable=True,
                                      server_default=sa.func.current_timestamp()))

    # ### end Alembic commands ###


def downgrade():

    with op.batch_alter_table('jobs_history', schema=None) as batch_op:
        batch_op.drop_column('updated_at')
        batch_op.drop_column('query_str')

    # ### end Alembic commands ###
