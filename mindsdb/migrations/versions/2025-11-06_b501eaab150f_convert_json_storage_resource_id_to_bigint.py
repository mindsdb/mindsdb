"""convert json_storage resource_id to bigint

Revision ID: b501eaab150f
Revises: 608e376c19a7
Create Date: 2025-11-06 16:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b501eaab150f'
down_revision = '608e376c19a7'
branch_labels = None
depends_on = None


def upgrade():
    """
    Convert resource_id column in json_storage table from Integer to BigInteger.
    This is necessary to support timestamp-based integration IDs that exceed 32-bit integer limits.
    """
    with op.batch_alter_table('json_storage', schema=None) as batch_op:
        batch_op.alter_column(
            'resource_id',
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=True
        )


def downgrade():
    """
    Revert resource_id column back to Integer.
    WARNING: This will fail if there are values > 2147483647 in the database.
    """
    with op.batch_alter_table('json_storage', schema=None) as batch_op:
        batch_op.alter_column(
            'resource_id',
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=True
        )
