"""telemetry_table

Revision ID: c898828b4a1b
Revises: 86b172b78a5b
Create Date: 2025-11-20 16:34:40.126294

"""

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

# revision identifiers, used by Alembic.
revision = "c898828b4a1b"
down_revision = "86b172b78a5b"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "telemetry",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("company_id", sa.String(), nullable=True),
        sa.Column("event", sa.String(), nullable=False),
        sa.Column("record", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("telemetry")
