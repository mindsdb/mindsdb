"""add_metadata_to_projects

Revision ID: f534e1c8409b
Revises: c06c35f7e8e1
Create Date: 2025-02-17 16:24:51.552182

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa



# revision identifiers, used by Alembic.
revision = 'f534e1c8409b'
down_revision = 'c06c35f7e8e1'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('project', schema=None) as batch_op:
        batch_op.add_column(sa.Column('metadata', sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table('project', schema=None) as batch_op:
        batch_op.drop_column('metadata')
