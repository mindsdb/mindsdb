"""drop-semaphor

Revision ID: 2958416fbe75
Revises: 9461892bd889
Create Date: 2024-04-25 18:30:54.051212

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = '2958416fbe75'
down_revision = '9461892bd889'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('semaphor')


def downgrade():
    op.create_table(
        'semaphor',
        sa.Column('id', sa.INTEGER(), nullable=False),
        sa.Column('updated_at', sa.DATETIME(), nullable=True),
        sa.Column('created_at', sa.DATETIME(), nullable=True),
        sa.Column('entity_type', sa.VARCHAR(), nullable=True),
        sa.Column('entity_id', sa.INTEGER(), nullable=True),
        sa.Column('action', sa.VARCHAR(), nullable=True),
        sa.Column('company_id', sa.INTEGER(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('entity_type', 'entity_id', name='uniq_const')
    )
