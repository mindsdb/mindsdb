"""projects

Revision ID: 43c52d23845a
Revises: cada7d2be947
Create Date: 2022-10-14 09:59:44.589745

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

import mindsdb.interfaces.storage.db as db


# revision identifiers, used by Alembic.
revision = '43c52d23845a'
down_revision = 'cada7d2be947'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'project',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('deleted_at', sa.DateTime(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'company_id', name='unique_project_name_company_id')
    )

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    project_record = db.Project(name='mindsdb')
    session.add(project_record)
    session.commit()

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('project_id', sa.Integer()))
        batch_op.create_foreign_key('fk_project_id', 'project', ['project_id'], ['id'])

    conn.execute(sa.sql.text('''
        update predictor set project_id = :project_id
    '''), project_id=project_record.id)

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.alter_column(
            'project_id',
            existing_type=sa.INTEGER(),
            nullable=False
        )

    with op.batch_alter_table('view', schema=None) as batch_op:
        batch_op.add_column(sa.Column('project_id', sa.Integer()))
        batch_op.create_foreign_key('fk_project_id', 'project', ['project_id'], ['id'])

    conn.execute(sa.sql.text('''
        update view set project_id = :project_id
    '''), project_id=project_record.id)

    with op.batch_alter_table('view', schema=None) as batch_op:
        batch_op.alter_column(
            'project_id',
            existing_type=sa.INTEGER(),
            nullable=False
        )

    views = conn.execute('''
        select id, name from view
        where exists (select 1 from predictor where view.name = predictor.name)
    ''').fetchall()

    for row in views:
        conn.execute(
            text("""
                update view
                set name = :name
                where id = :view_id
            """), {
                'name': f"{row['name']}_view",
                'view_id': row['id']
            }
        )

    session.commit()


def downgrade():
    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    view_integration = db.Integration.query.filter_by(name='views').first()
    if view_integration is None:
        views_integration = db.Integration(
            name='views',
            data={},
            engine='views',
            company_id=None
        )
        session.add(views_integration)
    session.commit()

    with op.batch_alter_table('view', schema=None) as batch_op:
        batch_op.drop_constraint('fk_project_id', type_='foreignkey')
        batch_op.drop_column('project_id')

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_constraint('fk_project_id', type_='foreignkey')
        batch_op.drop_column('project_id')

    op.drop_table('project')
