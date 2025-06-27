"""updated data catalog data types

Revision ID: 608e376c19a7
Revises: a44643042fe8
Create Date: 2025-06-09 23:20:34.739735

"""

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = "608e376c19a7"
down_revision = "a44643042fe8"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("meta_tables", schema=None) as batch_op:
        batch_op.alter_column(
            "row_count",
            type_=sa.BigInteger(),
            existing_type=sa.Integer(),
            existing_nullable=True,
            existing_server_default=None,
        )

    with op.batch_alter_table("meta_column_statistics", schema=None) as batch_op:
        batch_op.alter_column(
            "distinct_values_count",
            type_=sa.BigInteger(),
            existing_type=sa.Integer(),
            existing_nullable=True,
            existing_server_default=None,
        )


def downgrade():
    with op.batch_alter_table("meta_tables", schema=None) as batch_op:
        batch_op.alter_column(
            "row_count",
            type_=sa.Integer(),
            existing_type=sa.BigInteger(),
            existing_nullable=True,
            existing_server_default=None,
        )

    with op.batch_alter_table("meta_column_statistics", schema=None) as batch_op:
        batch_op.alter_column(
            "distinct_values_count",
            type_=sa.Integer(),
            existing_type=sa.BigInteger(),
            existing_nullable=True,
            existing_server_default=None,
        )
