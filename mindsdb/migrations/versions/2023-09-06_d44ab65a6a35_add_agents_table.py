"""add_agents_table

Revision ID: d44ab65a6a35
Revises: ad04ee0bd385
Create Date: 2023-09-06 11:32:08.777661

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd44ab65a6a35'
down_revision = 'ad04ee0bd385'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'agents',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.Column('user_class', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('model_name', sa.String(), nullable=False),
        sa.Column('params', sa.JSON(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'))


def downgrade():
    op.drop_table('agents')
