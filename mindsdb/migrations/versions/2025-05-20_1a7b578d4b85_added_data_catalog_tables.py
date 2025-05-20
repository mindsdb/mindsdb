"""added data catalog tables

Revision ID: 1a7b578d4b85
Revises: 53502b6d63bf
Create Date: 2025-05-20 13:51:28.562649

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa



# revision identifiers, used by Alembic.
revision = '1a7b578d4b85'
down_revision = '53502b6d63bf'
branch_labels = None
depends_on = None


def upgrade():
    # Create the meta_tables table
    op.create_table(
        'meta_tables',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('integration_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('schema', sa.String(), nullable=True),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('row_count', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ['integration_id'], ['integration.id'], name='fk_meta_tables_integration_id'
        ),
        sa.PrimaryKeyConstraint('id'),
    )

    # Create the meta_columns table
    op.create_table(
        'meta_columns',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('table_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('data_type', sa.String(), nullable=True),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('is_nullable', sa.Boolean(), nullable=True),
        sa.ForeignKeyConstraint(
            ['table_id'], ['meta_tables.id'], name='fk_meta_columns_table_id'
        ),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    # Drop the meta_columns table
    op.drop_table('meta_columns')

    # Drop the meta_tables table
    op.drop_table('meta_tables')
