"""straighten

Revision ID: 473e8f239481
Revises: 6a54ba55872e
Create Date: 2022-08-29 11:12:11.307317

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

import mindsdb.interfaces.storage.db    # noqa


# revision identifiers, used by Alembic.
revision = '473e8f239481'
down_revision = '6a54ba55872e'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)
    conn.execute(text('''
        delete from file
        where exists (
            select 1 from file as t2
            where file.name = t2.name
                  and file.company_id = t2.company_id
                  and file.id < t2.id
        )
    '''))
    session.commit()

    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.create_unique_constraint('unique_file_name_company_id', ['name', 'company_id'])

    conn.execute(text('''
        delete from integration where engine is null
    '''))
    session.commit()

    with op.batch_alter_table('integration', schema=None) as batch_op:
        batch_op.alter_column(
            'engine',
            existing_type=sa.VARCHAR(),
            nullable=False
        )

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_constraint('unique_predictor_name_company_id', type_='unique')

    conn.execute(text('''
        delete from semaphor
    '''))
    session.commit()

    try:
        with op.batch_alter_table('semaphor', schema=None) as batch_op:
            batch_op.create_unique_constraint('uniq_const', ['entity_type', 'entity_id'])
    except Exception:
        pass


def downgrade():
    with op.batch_alter_table('semaphor', schema=None) as batch_op:
        batch_op.drop_constraint('uniq_const', type_='unique')

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.create_unique_constraint('unique_predictor_name_company_id', ['name', 'company_id'])

    with op.batch_alter_table('integration', schema=None) as batch_op:
        batch_op.alter_column(
            'engine',
            existing_type=sa.VARCHAR(),
            nullable=True
        )

    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.drop_constraint('unique_file_name_company_id', type_='unique')
