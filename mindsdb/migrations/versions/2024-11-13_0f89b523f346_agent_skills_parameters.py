"""agent_skills_parameters

Revision ID: 0f89b523f346
Revises: 6c57ed39a82b
Create Date: 2024-11-13 15:24:39.796947

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

from sqlalchemy.dialects import sqlite  # noqa

# revision identifiers, used by Alembic.
revision = '0f89b523f346'
down_revision = '6c57ed39a82b'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('agent_skills', schema=None) as batch_op:
        batch_op.add_column(sa.Column('parameters', sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table('agent_skills', schema=None) as batch_op:
        batch_op.drop_column('parameters')
