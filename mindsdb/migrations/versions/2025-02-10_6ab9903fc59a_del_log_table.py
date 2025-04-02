"""del_log_table

Revision ID: 6ab9903fc59a
Revises: 4943359e354a
Create Date: 2025-02-10 16:50:27.186697

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

# revision identifiers, used by Alembic.
revision = '6ab9903fc59a'
down_revision = '4943359e354a'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('log')


def downgrade():
    op.create_table(
        'log',
        sa.Column('id', sa.INTEGER(), nullable=False),
        sa.Column('created_at', sa.DATETIME(), nullable=True),
        sa.Column('log_type', sa.VARCHAR(), nullable=True),
        sa.Column('source', sa.VARCHAR(), nullable=True),
        sa.Column('company_id', sa.INTEGER(), nullable=True),
        sa.Column('payload', sa.VARCHAR(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
