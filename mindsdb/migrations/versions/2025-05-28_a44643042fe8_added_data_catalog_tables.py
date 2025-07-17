"""added data catalog tables

Revision ID: a44643042fe8
Revises: 9f150e4f9a05
Create Date: 2025-05-28 17:20:57.300313

"""

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa
from mindsdb.interfaces.storage.db import Array


# revision identifiers, used by Alembic.
revision = "a44643042fe8"
down_revision = "9f150e4f9a05"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "meta_tables",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "integration_id",
            sa.Integer(),
            sa.ForeignKey("integration.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("schema", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("type", sa.String(), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
    )

    op.create_table(
        "meta_columns",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("meta_tables.id"), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("data_type", sa.String(), nullable=False),
        sa.Column("default_value", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("is_nullable", sa.Boolean(), nullable=True),
    )

    op.create_table(
        "meta_column_statistics",
        sa.Column(
            "column_id",
            sa.Integer(),
            sa.ForeignKey("meta_columns.id"),
            primary_key=True,
        ),
        sa.Column("most_common_values", Array(), nullable=True),
        sa.Column("most_common_frequencies", Array(), nullable=True),
        sa.Column("null_percentage", sa.Numeric(5, 2), nullable=True),
        sa.Column("distinct_values_count", sa.Integer(), nullable=True),
        sa.Column("minimum_value", sa.String(), nullable=True),
        sa.Column("maximum_value", sa.String(), nullable=True),
    )

    op.create_table(
        "meta_primary_keys",
        sa.Column("table_id", sa.Integer(), sa.ForeignKey("meta_tables.id"), primary_key=True),
        sa.Column(
            "column_id",
            sa.Integer(),
            sa.ForeignKey("meta_columns.id"),
            primary_key=True,
        ),
        sa.Column("ordinal_position", sa.Integer(), nullable=True),
        sa.Column("constraint_name", sa.String(), nullable=True),
    )

    op.create_table(
        "meta_foreign_keys",
        sa.Column(
            "parent_table_id",
            sa.Integer(),
            sa.ForeignKey("meta_tables.id"),
            primary_key=True,
        ),
        sa.Column(
            "parent_column_id",
            sa.Integer(),
            sa.ForeignKey("meta_columns.id"),
            primary_key=True,
        ),
        sa.Column(
            "child_table_id",
            sa.Integer(),
            sa.ForeignKey("meta_tables.id"),
            primary_key=True,
        ),
        sa.Column(
            "child_column_id",
            sa.Integer(),
            sa.ForeignKey("meta_columns.id"),
            primary_key=True,
        ),
        sa.Column("constraint_name", sa.String(), nullable=True),
    )


def downgrade():
    op.drop_table("meta_tables")

    op.drop_table("meta_columns")

    op.drop_table("meta_column_statistics")

    op.drop_table("meta_primary_keys")

    op.drop_table("meta_foreign_keys")
