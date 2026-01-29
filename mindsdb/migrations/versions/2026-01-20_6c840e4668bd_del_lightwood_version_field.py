"""del_lightwood_version_field

Revision ID: 6c840e4668bd
Revises: f64112749455
Create Date: 2026-01-20 18:05:30.706658

"""

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

# revision identifiers, used by Alembic.
revision = "6c840e4668bd"
down_revision = "f64112749455"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("predictor", schema=None) as batch_op:
        batch_op.drop_column("native_version")
        batch_op.drop_column("lightwood_version")


def downgrade():
    with op.batch_alter_table("predictor", schema=None) as batch_op:
        batch_op.add_column(sa.Column("lightwood_version", sa.VARCHAR(), nullable=True))
        batch_op.add_column(sa.Column("native_version", sa.VARCHAR(), nullable=True))
