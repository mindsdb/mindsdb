"""add agent_skills_table

Revision ID: e187961e844a
Revises: 4c26ad04eeaa
Create Date: 2023-09-06 13:56:17.803484

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e187961e844a'
down_revision = '4c26ad04eeaa'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'agent_skills',
        sa.Column('agent_id', sa.Integer(), nullable=False),
        sa.Column('skill_id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('agent_id', 'skill_id')
    )


def downgrade():
    op.drop_table('agent_skills')
