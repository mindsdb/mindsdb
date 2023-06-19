"""create_chat_bots_history_table

Revision ID: b5bf593ba659
Revises: 9d6271bb2c38
Create Date: 2023-06-19 09:56:37.108680

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b5bf593ba659'
down_revision = '9d6271bb2c38'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'chat_bots_history',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('chat_bot_id', sa.Integer(), nullable=False),
        sa.Column('type', sa.String()),
        sa.Column('text', sa.String()),
        sa.Column('user', sa.String()),
        sa.Column('destination', sa.String()),
        sa.Column('sent_at', sa.DateTime()),
        sa.Column('error', sa.String()),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade():
    op.drop_table('chat_bots_history')
