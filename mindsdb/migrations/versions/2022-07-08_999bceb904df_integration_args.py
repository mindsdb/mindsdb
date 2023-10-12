"""integration-args

Revision ID: 999bceb904df
Revises: d74c189b87e6
Create Date: 2022-07-08 10:58:19.822618

"""
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

import mindsdb.interfaces.storage.db    # noqa


# revision identifiers, used by Alembic.
revision = '999bceb904df'
down_revision = 'd74c189b87e6'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    with op.batch_alter_table('integration', schema=None) as batch_op:
        batch_op.add_column(sa.Column('engine', sa.String()))

    integrations = conn.execute(text('''
        select id, name, data from integration
    ''')).fetchall()

    for row in integrations:
        try:
            data = json.loads(row['data'])
        except Exception:
            data = {}

        if 'test' in data:
            del data['test']
        if 'publish' in data:
            del data['publish']
        if 'enabled' in data:
            del data['enabled']
        if 'database_name' in data:
            if row['name'] is None:
                row['name'] = data['database_name']
            del data['database_name']
        integration_type = data.get('type')
        if integration_type is None:
            if row['name'] == 'files':
                integration_type = 'files'
            if row['name'] == 'views':
                integration_type = 'views'
        if 'type' in data:
            del data['type']

        conn.execute(
            text("""
                update integration
                set engine = :integration_type,
                    data = :integration_data
                where id = :integration_id
            """), {
                'integration_type': integration_type,
                'integration_data': json.dumps(data),
                'integration_id': row['id']
            }
        )

    session.commit()


def downgrade():
    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    integrations = conn.execute(sa.text('''
        select id, name, type, data from integration
    ''')).fetchall()

    for row in integrations:
        try:
            data = json.loads(row['data'])
        except Exception:
            data = {}
        if row['engine'] is not None:
            data['type'] = row['engine']

        conn.execute(
            text("""
                update integration
                set data = :integration_data
                where id = :integration_id
            """), {
                'integration_data': json.dumps(data),
                'integration_id': row['id']
            }
        )

    with op.batch_alter_table('integration', schema=None) as batch_op:
        batch_op.drop_column('engine')

    session.commit()
