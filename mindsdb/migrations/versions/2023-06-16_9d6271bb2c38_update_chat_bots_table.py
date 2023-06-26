"""update_chat_bots_table

Revision ID: 9d6271bb2c38
Revises: aaecd7012a78
Create Date: 2023-06-16 12:08:50.170809

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9d6271bb2c38'
down_revision = 'aaecd7012a78'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('chat_bots', schema=None) as batch_op:
        batch_op.add_column(sa.Column('chat_engine', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('is_running', sa.Boolean(), default=True))


def downgrade():
    with op.batch_alter_table('chat_bots', schema=None) as batch_op:
        batch_op.drop_column('chat_engine')
        batch_op.drop_column('is_running')
