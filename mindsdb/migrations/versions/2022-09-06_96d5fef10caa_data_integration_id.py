"""data_integration_id

Revision ID: 96d5fef10caa
Revises: 473e8f239481
Create Date: 2022-09-06 15:20:02.382203

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

import mindsdb.interfaces.storage.db        # noqa


# revision identifiers, used by Alembic.
revision = '96d5fef10caa'
down_revision = '473e8f239481'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('data_integration_id', sa.Integer(), nullable=True))

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)
    result = conn.execute(text('''
        select 1 from integration where name = 'lightwood';
    ''')).fetchall()
    if len(result) == 0:
        conn.execute(text('''
            insert into integration (name, engine, data)
            values ('lightwood', 'lightwood', '{}')
        '''))
    conn.execute(text('''
        update predictor set data_integration_id = integration_id
        where exists (select 1 from integration where integration.id = predictor.integration_id);
    '''))
    conn.execute(text('''
        update predictor set integration_id = (select id from integration where name = 'lightwood');
    '''))
    session.commit()

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.alter_column(
            'integration_id',
            existing_type=sa.INTEGER(),
            nullable=False
        )
        batch_op.create_foreign_key('fk_data_integration_id', 'integration', ['data_integration_id'], ['id'])


def downgrade():
    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)
    conn.execute(text('''
        update predictor set integration_id = data_integration_id;
    '''))
    session.commit()

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_constraint('fk_data_integration_id', type_='foreignkey')
        batch_op.alter_column(
            'integration_id',
            existing_type=sa.INTEGER(),
            nullable=True
        )
        batch_op.drop_column('data_integration_id')
