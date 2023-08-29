"""predictor-hostname

Revision ID: b0382f5be48d
Revises: ad04ee0bd385
Create Date: 2023-08-29 17:19:55.372394

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b0382f5be48d'
down_revision = 'ad04ee0bd385'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('hostname', sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('hostname')
