"""query_database

Revision ID: 53502b6d63bf
Revises: fda503400e43
Create Date: 2025-04-22 16:30:15.139978

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = '53502b6d63bf'
down_revision = 'fda503400e43'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('queries', schema=None) as batch_op:
        batch_op.add_column(sa.Column('database', sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table('queries', schema=None) as batch_op:
        batch_op.drop_column('database')
