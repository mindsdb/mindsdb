"""predictors-versioning

Revision ID: 976f15a37e6a
Revises: 6e834843e7e9
Create Date: 2022-08-19 11:22:52.085339

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

import mindsdb.interfaces.storage.db    # noqa


# revision identifiers, used by Alembic.
revision = '976f15a37e6a'
down_revision = '6e834843e7e9'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('deleted_at', sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column('active', sa.Boolean(), nullable=True))

    conn.execute(text('''
        update predictor set active = :val;
    '''), {
        'val': True
    })

    session.commit()


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('active')
        batch_op.drop_column('deleted_at')
