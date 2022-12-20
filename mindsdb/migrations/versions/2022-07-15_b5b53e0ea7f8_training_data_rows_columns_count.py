"""training_data_rows_columns_count

Revision ID: b5b53e0ea7f8
Revises: 999bceb904df
Create Date: 2022-07-15 12:21:40.523039

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db    # noqa


# revision identifiers, used by Alembic.
revision = 'b5b53e0ea7f8'
down_revision = '999bceb904df'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('training_data_columns_count', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('training_data_rows_count', sa.Integer(), nullable=True))


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('training_data_rows_count')
        batch_op.drop_column('training_data_columns_count')
