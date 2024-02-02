"""add_fine_tuning_jobs_table

Revision ID: 18304328b2ef
Revises: 4b3c9d63e89c
Create Date: 2024-02-02 12:41:34.598315

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db # noqa


# revision identifiers, used by Alembic.
revision = '18304328b2ef'
down_revision = '4b3c9d63e89c'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'fine_tuning_jobs',
        sa.Column('id', sa.INTEGER(), nullable=False),
        sa.Column('model_id', sa.INTEGER(), nullable=False),
        sa.Column('training_file', sa.VARCHAR(), nullable=False),
        sa.Column('validation_file', sa.VARCHAR(), nullable=True),
        sa.Column('created_at', sa.DATETIME(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('fine_tuning_jobs')
