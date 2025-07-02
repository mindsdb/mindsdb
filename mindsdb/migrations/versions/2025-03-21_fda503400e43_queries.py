"""queries

Revision ID: fda503400e43
Revises: 11347c213b36
Create Date: 2025-03-21 18:50:20.795930

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = 'fda503400e43'
down_revision = '11347c213b36'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'queries',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.Column('sql', sa.String(), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('parameters', sa.JSON(), nullable=True),
        sa.Column('context', sa.JSON(), nullable=True),
        sa.Column('processed_rows', sa.Integer(), nullable=True),
        sa.Column('error', sa.String(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    with op.batch_alter_table('knowledge_base', schema=None) as batch_op:
        batch_op.add_column(sa.Column('query_id', sa.INTEGER(), nullable=True))


def downgrade():
    with op.batch_alter_table('knowledge_base', schema=None) as batch_op:
        batch_op.drop_column('query_id')

    op.drop_table('queries')
