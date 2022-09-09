"""predictor-status

Revision ID: 87b2df2b83e1
Revises: 96d5fef10caa
Create Date: 2022-09-08 14:47:45.238710

"""
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

import mindsdb.interfaces.storage.db    # noqa


# revision identifiers, used by Alembic.
revision = '87b2df2b83e1'
down_revision = '96d5fef10caa'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('status', sa.String(), nullable=True))

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)
    predictors = conn.execute('''
        select id, data, update_status, json_ai, code from predictor
    ''').fetchall()

    for row in predictors:
        try:
            data = json.loads(row['data'])
        except Exception:
            data = None

        status = None
        # assume older models are complete, only temporary
        if 'status' in (data or {}):
            status = data['status']
        elif 'error' in (data or {}):
            status = 'error'
        elif row['update_status'] == 'available':
            status = 'complete'
        elif row['json_ai'] is None and row['code'] is None:
            status = 'generating'
        elif data is None:
            status = 'error'
        elif 'training_log' in (data or {}):
            status = 'training'
        elif 'error' not in (data or {}):
            status = 'complete'
        else:
            status = 'error'

        conn.execute(
            text("""
                update predictor
                set status = :status
                where id = :predictor_id
            """), {
                'status': status,
                'predictor_id': row['id']
            }
        )
    session.commit()


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('status')
