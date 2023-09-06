"""add_skills_table

Revision ID: 4c26ad04eeaa
Revises: b5bf593ba659
Create Date: 2023-08-31 17:04:26.898015

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4c26ad04eeaa'
down_revision = 'd44ab65a6a35'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'skills',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('type', sa.String(), nullable=False),
        sa.Column('params', sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('skills')
