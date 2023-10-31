"""add_agent_id

Revision ID: f16d4ab03091
Revises: e187961e844a
Create Date: 2023-09-18 10:49:36.290319

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f16d4ab03091'
down_revision = 'e187961e844a'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('chat_bots') as batch_op:
        batch_op.add_column(sa.Column('agent_id', sa.Integer()))
        batch_op.create_foreign_key('fk_agent_id', 'agents', ['agent_id'], ['id'])


def downgrade():
    with op.batch_alter_table('chat_bots') as batch_op:
        batch_op.drop_constraint('fk_agent_id', type_='foreignkey')
        batch_op.drop_column('agent_id')
