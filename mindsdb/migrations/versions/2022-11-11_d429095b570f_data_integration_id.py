"""data-integration-id

Revision ID: d429095b570f
Revises: 1e60096fc817
Create Date: 2022-11-11 14:00:58.386307

"""
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

import mindsdb.interfaces.storage.db as db


# revision identifiers, used by Alembic.
revision = 'd429095b570f'
down_revision = '1e60096fc817'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('data_integration_ref', db.Json(), nullable=True))

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    view_integration = conn.execute('''
        select id from integration where name = 'views'
    ''').fetchone()
    if view_integration is not None:
        views_integration_id = view_integration['id']

        predictors = conn.execute('''
            select id, data_integration_id from predictor
        ''').fetchall()

        for predictor in predictors:
            data_integration_ref = None
            if predictor['data_integration_id'] is not None:
                data_integration_ref = {'type': 'integration', 'id': predictor['data_integration_id']}
            if predictor['data_integration_id'] == views_integration_id:
                data_integration_ref = {'type': 'view'}
            if isinstance(data_integration_ref, dict):
                data_integration_ref = json.dumps(data_integration_ref)
            conn.execute(text('''
                update predictor set data_integration_ref = :data_integration_ref where id = :id
            '''), {
                'data_integration_ref': data_integration_ref,
                'id': predictor['id']
            })

    session.commit()

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_constraint('fk_data_integration_id', type_='foreignkey')
        batch_op.drop_column('data_integration_id')

    conn.execute('''
        delete from integration where name = 'views'
    ''')
    session.commit()


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('data_integration_id', sa.INTEGER(), nullable=True))

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    views_integration = db.Integration(
        name='views',
        data={},
        engine='views',
        company_id=None
    )
    session.add(views_integration)
    session.commit()

    predictors = conn.execute('''
        select id, data_integration_ref from predictor
    ''').fetchall()

    for predictor in predictors:
        data_integration_ref = predictor['data_integration_ref']
        if data_integration_ref is None:
            continue
        data_integration_ref = json.loads(data_integration_ref)
        data_integration_id = data_integration_ref.get('id')
        if data_integration_ref['type'] == 'view':
            data_integration_id = views_integration.id

        conn.execute(text('''
            update predictor set data_integration_id = :data_integration_id where id = :id
        '''), {
            'data_integration_id': data_integration_id,
            'id': predictor['id']
        })

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.create_foreign_key('fk_data_integration_id', 'integration', ['data_integration_id'], ['id'])
        batch_op.drop_column('data_integration_ref')
