"""added data catalog tables

Revision ID: 1a7b578d4b85
Revises: 53502b6d63bf
Create Date: 2025-05-20 13:51:28.562649

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa
from mindsdb.interfaces.storage.db import Array



# revision identifiers, used by Alembic.
revision = '1a7b578d4b85'
down_revision = '53502b6d63bf'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'meta_tables',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('integration_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('schema', sa.String(), nullable=True),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('type', sa.String(), nullable=True),
        sa.Column('row_count', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(
            ['integration_id'], ['integration.id'], name='fk_meta_tables_integration_id'
        ),
    )

    op.create_table(
        'meta_columns',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('table_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('data_type', sa.String(), nullable=True),
        sa.Column('default_value', sa.String(), nullable=True),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('is_nullable', sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(
            ['table_id'], ['meta_tables.id'], name='fk_meta_columns_table_id'
        ),
    )

    op.create_table(
        'meta_column_statistics',
        sa.Column('column_id', sa.Integer(), sa.ForeignKey('meta_columns.id'), primary_key=True),
        sa.Column('most_common_values', Array(), nullable=True),
        sa.Column('most_common_frequencies', Array(), nullable=True),
        sa.Column('null_percentage', sa.Numeric(5, 2), nullable=True),
        sa.Column('distinct_values_count', sa.Integer(), nullable=True),
        sa.Column('minimum_value', sa.String(), nullable=True),
        sa.Column('maximum_value', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('column_id'),
        sa.ForeignKeyConstraint(
            ['column_id'], ['meta_columns.id'], name='fk_meta_column_statistics_column_id'
        ),
    )
    
    op.create_table(
        'meta_primary_keys',
        sa.Column('table_id', sa.Integer(), nullable=True),
        sa.Column('column_id', sa.Integer(), nullable=True),
        sa.Column('constraint_name', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('table_id', 'column_id'),
        sa.ForeignKeyConstraint(
            ['table_id'], ['meta_tables.id'], name='fk_meta_primary_keys_table_id'
        ),
        sa.ForeignKeyConstraint(
            ['column_id'], ['meta_columns.id'], name='fk_meta_primary_keys_column_id'
        ),
    )


def downgrade():
    op.drop_table('meta_columns')

    op.drop_table('meta_tables')

    op.drop_table('meta_tables')

    op.drop_table('meta_column_statistics')
