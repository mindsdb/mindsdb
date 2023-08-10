"""json_storage

Revision ID: cada7d2be947
Revises: 3d5e70105df7
Create Date: 2022-09-29 15:52:32.695026

"""
import json

from alembic import op
import sqlalchemy as sa

import mindsdb.interfaces.storage.db as db
from mindsdb.interfaces.storage.fs import RESOURCE_GROUP


# revision identifiers, used by Alembic.
revision = 'cada7d2be947'
down_revision = '3d5e70105df7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'json_storage',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('resource_group', sa.String(), nullable=True),
        sa.Column('resource_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('content', sa.JSON(), nullable=True),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)
    predictors = conn.execute(sa.text('''
        select id, json_ai from predictor
    ''')).fetchall()

    for row in predictors:
        try:
            jai = json.loads(row['json_ai'])
        except Exception:
            continue

        if jai is None or len(jai) == 0:
            continue

        record = db.JsonStorage(
            resource_group=RESOURCE_GROUP.PREDICTOR,
            resource_id=row['id'],
            name='json_ai',
            content=jai,
            company_id=None
        )
        session.add(record)

    session.commit()

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('json_ai')


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('json_ai', sa.JSON, nullable=True))

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)
    jsons = conn.execute(sa.text('''
        select resource_id, name, content
        from json_storage
        where resource_group = 'predictor' and name = 'json_ai'
    ''')).fetchall()

    for row in jsons:
        predicrtor_record = (
            session.query(db.Predictor)
            .filter_by(company_id=None, id=row['resource_id'])
            .first()
        )
        if predicrtor_record is None:
            continue
        predicrtor_record.json_ai = row['content']

    session.commit()

    op.drop_table('json_storage')
