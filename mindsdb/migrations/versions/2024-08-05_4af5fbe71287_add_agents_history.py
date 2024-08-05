"""add_agents_history

Revision ID: 4af5fbe71287
Revises: 45eb2eb61f70
Create Date: 2024-08-05 11:22:56.297128

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

# revision identifiers, used by Alembic.
revision = '4af5fbe71287'
down_revision = '45eb2eb61f70'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('agents_history',
                    sa.Column('id', sa.INTEGER(), nullable=False),
                    sa.Column('agent_id', sa.INTEGER(), nullable=False),
                    sa.Column('type', sa.VARCHAR(), nullable=True),
                    sa.Column('text', sa.VARCHAR(), nullable=True),
                    sa.Column('sender_type', sa.VARCHAR(), nullable=True),
                    sa.Column('destination', sa.VARCHAR(), nullable=True),
                    sa.Column('sent_at', sa.DATETIME(), nullable=True),
                    sa.Column('error', sa.VARCHAR(), nullable=True),
                    sa.Column('trace_id', sa.VARCHAR(), nullable=True),
                    sa.Column('observation_id', sa.VARCHAR(), nullable=True),
                    sa.Column('tools', sa.VARCHAR(), nullable=True),
                    sa.Column('stream_id', sa.VARCHAR(), nullable=True),
                    sa.ForeignKeyConstraint(['agent_id'], ['agents.id'], name='fk_agent_id'),
                    sa.PrimaryKeyConstraint('id')
                    )

    # ### end Alembic commands ###


def downgrade():
    op.drop_table('agents_history')
