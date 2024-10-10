"""added webhook_token to chat_bots

Revision ID: 6c57ed39a82b
Revises: 8e17ff6b75e9
Create Date: 2024-10-07 16:40:14.141878

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = '6c57ed39a82b'
down_revision = '8e17ff6b75e9'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('chat_bots', schema=None) as batch_op:
        batch_op.add_column(sa.Column('webhook_token', sa.VARCHAR(), nullable=True))


def downgrade():
    with op.batch_alter_table('chat_bots', schema=None) as batch_op:
        batch_op.drop_column('webhook_token')
