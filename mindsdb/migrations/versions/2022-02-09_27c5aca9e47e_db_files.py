"""db files

Revision ID: 27c5aca9e47e
Revises: 47f97b83cee4
Create Date: 2022-02-09 10:43:29.854671

"""
import json
import datetime

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db
from sqlalchemy.sql import text


# revision identifiers, used by Alembic.
revision = '27c5aca9e47e'
down_revision = '47f97b83cee4'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('ai_table')

    conn = op.get_bind()

    # views was created with unnamed fk. Therefore need recreate it
    op.create_table(
        'view_tmp',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.Column('query', sa.String(), nullable=False),
        sa.Column('integration_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['integration_id'], ['integration.id'], name='fk_integration_id'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'company_id', name='unique_view_name_company_id')
    )
    conn.execute(text("""
        insert into view_tmp (id, name, company_id, query, integration_id)
        select id, name, company_id, query, datasource_id from view;
    """))
    op.drop_table('view')
    op.rename_table('view_tmp', 'view')

    op.create_table(
        'analysis',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('analysis', mindsdb.interfaces.storage.db.Json(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    with op.batch_alter_table('datasource', schema=None) as batch_op:
        batch_op.add_column(sa.Column('analysis_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key('fk_analysis_id', 'analysis', ['analysis_id'], ['id'])
        batch_op.add_column(sa.Column('ds_class', sa.String(), nullable=True))

    session = sa.orm.Session(bind=conn)
    dsatasources = conn.execute('select id, analysis from datasource').fetchall()
    for row in dsatasources:
        if row['analysis'] is not None:
            # NOTE 'returning' is relatively new in sqlite, so better will be use select after insert.
            conn.execute(
                text("""
                    insert into analysis (analysis) select analysis from datasource where id = :id;
                """), {
                    'id': row['id']
                }
            )
            analysis_id = conn.execute(text("""
                select id from analysis order by id desc limit 1;
            """)).fetchall()
            conn.execute(
                text("""
                    update datasource set analysis_id = :analysis_id where id = :id
                """), {
                    'analysis_id': analysis_id[0][0],
                    'id': row['id']
                }
            )

    with op.batch_alter_table('datasource', schema=None) as batch_op:
        batch_op.drop_column('analysis')

    op.create_table(
        'file',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.Column('source_file_path', sa.String(), nullable=False),
        sa.Column('file_path', sa.String(), nullable=False),
        sa.Column('row_count', sa.Integer(), nullable=False),
        sa.Column('columns', mindsdb.interfaces.storage.db.Json(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.Column('updated_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp(), server_onupdate=sa.func.current_timestamp()),
        sa.Column('analysis_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['analysis_id'], ['analysis.id'], name='fk_analysis_id'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'company_id', name='unique_file_name_company_id')
    )

    # delete ds where data is none
    dsatasources = conn.execute(text('select * from datasource')).fetchall()
    for ds in dsatasources:
        if ds['data'] is None:
            conn.execute(text('delete from datasource where id = :id'), {'id': ds['id']})
            continue
        ds_data = json.loads(ds['data'])
        creation_info = json.loads(ds['creation_info'])
        datasource_name = ds_data.get('source_type')
        if datasource_name == 'file':
            created_at = None
            if isinstance(ds['created_at'], str):
                created_at = datetime.datetime.fromisoformat(ds['created_at'])
            elif isinstance(ds['created_at'], [float, int]):
                created_at = datetime.fromtimestamp(ds['created_at'])

            updated_at = None
            if isinstance(ds['updated_at'], str):
                updated_at = datetime.datetime.fromisoformat(ds['updated_at'])
            elif isinstance(ds['updated_at'], [float, int]):
                updated_at = datetime.fromtimestamp(ds['updated_at'])

            file = mindsdb.interfaces.storage.db.File(
                name=ds['name'],
                company_id=ds['company_id'],
                source_file_path=ds_data['source'],
                file_path=creation_info['args'][0],
                row_count=ds_data['row_count'],
                columns=ds_data['columns'],
                created_at=created_at,
                updated_at=updated_at,
                analysis_id=ds['analysis_id']
            )
            session.add(file)
            session.flush()
            # ds_data['file_id'] = file.id
            ds_data['source'] = {
                'mindsdb_file_name': ds['name']
                # 'source': ds_data['source']
            }
            conn.execute(
                text("""
                    update datasource set data = :ds_data where id = :id;
                """), {
                    'id': ds['id'],
                    'ds_data': json.dumps(ds_data)
                }
            )

        conn.execute(
            text("""
                update datasource
                set integration_id = (select id from integration where name = :datasource_name and company_id = :company_id),
                    ds_class = :ds_class
                where id = :id
            """), {
                'datasource_name': datasource_name,
                'company_id': ds['company_id'],
                'ds_class': creation_info['class'],
                'id': ds['id']
            }
        )

    session.commit()

    op.rename_table('datasource', 'dataset')

    with op.batch_alter_table('dataset', schema=None) as batch_op:
        batch_op.create_foreign_key('fk_integration_id', 'integration', ['integration_id'], ['id'])

    # NOTE two different 'batch' is necessary, in other way FK is not creating
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.alter_column('datasource_id', new_column_name='dataset_id')
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.create_foreign_key('fk_predictor_dataset_id', 'dataset', ['dataset_id'], ['id'])
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.create_unique_constraint('unique_predictor_name_company_id', ['name', 'company_id'])

    with op.batch_alter_table('integration', schema=None) as batch_op:
        batch_op.create_unique_constraint('unique_integration_name_company_id', ['name', 'company_id'])

    with op.batch_alter_table('dataset', schema=None) as batch_op:
        batch_op.create_unique_constraint('unique_dataset_name_company_id', ['name', 'company_id'])


def downgrade():
    with op.batch_alter_table('integration', schema=None) as batch_op:
        batch_op.drop_constraint('unique_integration_name_company_id', type_='unique')

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_constraint('unique_predictor_name_company_id', type_='unique')

    with op.batch_alter_table('dataset', schema=None) as batch_op:
        batch_op.drop_constraint('unique_dataset_name_company_id', type_='unique')

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_constraint('fk_predictor_dataset_id', type_='foreignkey')
        batch_op.alter_column('dataset_id', new_column_name='datasource_id')

    with op.batch_alter_table('dataset', schema=None) as batch_op:
        batch_op.drop_constraint('fk_integration_id', type_='foreignkey')
        batch_op.add_column(sa.Column('analysis', sa.VARCHAR(), nullable=True))
        batch_op.drop_constraint('fk_analysis_id', type_='foreignkey')
        batch_op.drop_column('ds_class')

    op.rename_table('dataset', 'datasource')

    op.drop_table('file')

    conn = op.get_bind()
    conn.execute(text("""
        update datasource set analysis = (select analysis from analysis where id = analysis_id)
    """))

    with op.batch_alter_table('datasource', schema=None) as batch_op:
        batch_op.drop_column('analysis_id')

    op.drop_table('analysis')

    op.create_table(
        'ai_table',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('integration_name', sa.String(), nullable=False),
        sa.Column('integration_query', sa.String(), nullable=False),
        sa.Column('query_fields', mindsdb.interfaces.storage.db.Json(), nullable=False),
        sa.Column('predictor_name', sa.String(), nullable=False),
        sa.Column('predictor_columns', mindsdb.interfaces.storage.db.Json(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'view_tmp',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=True),
        sa.Column('query', sa.String(), nullable=False),
        sa.Column('datasource_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['datasource_id'], ['integration.id'], name='fk_datasource_id'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'company_id', name='unique_name_company_id')
    )
    conn.execute(text("""
        insert into view_tmp (id, name, company_id, query, datasource_id)
        select id, name, company_id, query, integration_id from view;
    """))
    op.drop_table('view')
    op.rename_table('view_tmp', 'view')
