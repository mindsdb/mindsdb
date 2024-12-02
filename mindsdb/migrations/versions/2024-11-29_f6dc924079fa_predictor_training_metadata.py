"""predictor_training_metadata

Revision ID: f6dc924079fa
Revises: a8a3fac369e7
Create Date: 2024-11-29 15:06:47.269229

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa

# revision identifiers, used by Alembic.
revision = 'f6dc924079fa'
down_revision = 'a8a3fac369e7'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('training_metadata', sa.JSON(), nullable=True))
        batch_op.drop_column('is_custom')
        batch_op.drop_column('hostname')

    predictor_table = sa.Table(
        'predictor',
        sa.MetaData(),
        sa.Column('id', sa.Integer()),
        sa.Column('training_metadata', sa.JSON())
    )

    op.execute(predictor_table.update().values(training_metadata={}))
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.alter_column(
            'training_metadata',
            nullable=False
        )


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('hostname', sa.VARCHAR(), nullable=True))
        batch_op.add_column(sa.Column('is_custom', sa.BOOLEAN(), nullable=True))
        batch_op.drop_column('training_metadata')
