"""store llm data

Revision ID: cbedc4968d5d
Revises: 2958416fbe75
Create Date: 2024-06-06 13:59:45.158089

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = 'cbedc4968d5d'
down_revision = '2958416fbe75'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'llm_data',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('input', sa.String(), nullable=False),
        sa.Column('output', sa.String(), nullable=False),
        sa.Column('model_id', sa.String(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('llm_data')
