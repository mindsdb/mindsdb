"""add_llm_log_columns

Revision ID: 9d559f68d535
Revises: 6c57ed39a82b
Create Date: 2024-11-15 11:24:28.808881

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = '9d559f68d535'
down_revision = '6c57ed39a82b'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('llm_log', schema=None) as batch_op:
        batch_op.alter_column('company_id', nullable=False)
        batch_op.alter_column('model_id', nullable=True)
        batch_op.add_column(sa.Column('model_group', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('cost', sa.Numeric(), nullable=True))
        batch_op.add_column(sa.Column('exception', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('traceback', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('stream', sa.Boolean(), default=False))
        batch_op.add_column(sa.Column('metadata', sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table('llm_log', schema=None) as batch_op:
        batch_op.alter_column('company_id', nullable=True)
        batch_op.alter_column('model_id', nullable=False)
        batch_op.drop_column('model_group')
        batch_op.drop_column('cost')
        batch_op.drop_column('exception')
        batch_op.drop_column('traceback')
        batch_op.drop_column('stream')
        batch_op.drop_column('metadata')
