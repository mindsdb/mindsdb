"""add-skills-metadata

Revision ID: b6788e1822c9
Revises: 11347c213b36
Create Date: 2025-03-03 18:40:12.540208

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

from sqlalchemy.dialects import sqlite  # noqa

# revision identifiers, used by Alembic.
revision = 'b6788e1822c9'
down_revision = '11347c213b36'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('skills', schema=None) as batch_op:
        batch_op.add_column(sa.Column('metadata', sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table('skills', schema=None) as batch_op:
        batch_op.drop_column('metadata')
