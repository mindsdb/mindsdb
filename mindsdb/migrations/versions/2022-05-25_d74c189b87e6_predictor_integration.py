"""predictor-integration

Revision ID: d74c189b87e6
Revises: 27c5aca9e47e
Create Date: 2022-05-25 15:00:16.284158

"""
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text


# revision identifiers, used by Alembic.
revision = 'd74c189b87e6'
down_revision = '27c5aca9e47e'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('integration_id', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('fetch_data_query', sa.String(), nullable=True))
        batch_op.create_foreign_key('fk_integration_id', 'integration', ['integration_id'], ['id'])

    conn = op.get_bind()

    conn.execute(text('''
        insert into integration (name, data, company_id, created_at, updated_at)
            select
                'files' as name,
                '{}' as data,
                company_id,
                '2022-05-01 00:00:00.000000' as created_at,
                '2022-05-01 00:00:00.000000' as updated_at
            from (select distinct company_id from integration) t1
    '''))

    predictors = conn.execute(text('''
        select t1.id, t1.company_id, t2.data
        from predictor t1 left join dataset t2 on t1.id = t2.id
        where dataset_id is not null and t2.data is not null
    ''')).fetchall()
    for row in predictors:
        data = row['data']
        try:
            data = json.loads(data)
        except Exception:
            continue

        if 'source_type' not in data:
            continue
        integration_name = data.get('source_type')
        if isinstance(integration_name, str) is False or len(integration_name) == 0:
            continue
        if integration_name.lower() == 'file':
            integration_name = 'files'

        fetch_data_query = data.get('source')
        if isinstance(fetch_data_query, dict) is False:
            continue

        if integration_name == 'files':
            file_name = fetch_data_query.get('mindsdb_file_name')
            if isinstance(file_name, str) is False or len(file_name) == 0:
                continue
            fetch_data_query = f'select * from {file_name}'
        else:
            fetch_data_query = fetch_data_query.get('query')
            if isinstance(fetch_data_query, str) is False or len(fetch_data_query) == 0:
                continue

        query = '''
            select id
            from integration
            where company_id = :company_id and lower(name) = lower(:name)
        '''
        if row['company_id'] is None:
            query = '''
                select id
                from integration
                where company_id is null and lower(name) = lower(:name)
            '''
        integration = conn.execute(text(query), {
            'company_id': row['company_id'],
            'name': integration_name
        }).fetchone()
        if integration is None:
            continue

        conn.execute(text('''
            update predictor
            set integration_id = :integration_id, fetch_data_query = :fetch_data_query
            where id = :predictor_id
        '''), {
            'integration_id': integration.id,
            'fetch_data_query': fetch_data_query,
            'predictor_id': row['id']
        })

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_column('dataset_id')
    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.drop_constraint('unique_file_name_company_id', type_='unique')
        batch_op.drop_constraint('fk_analysis_id', type_='foreignkey')
        batch_op.drop_column('analysis_id')
    op.drop_table('analysis')
    op.drop_table('dataset')


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('dataset_id', sa.INTEGER(), nullable=True))
        batch_op.drop_constraint('fk_integration_id', type_='foreignkey')
        batch_op.create_foreign_key('fk_predictor_dataset_id', 'dataset', ['dataset_id'], ['id'])
        batch_op.drop_column('fetch_data_query')
        batch_op.drop_column('integration_id')

    op.create_table(
        'analysis',
        sa.Column('id', sa.INTEGER(), nullable=False),
        sa.Column('analysis', sa.VARCHAR(), nullable=False),
        sa.Column('created_at', sa.DATETIME(), nullable=True),
        sa.Column('updated_at', sa.DATETIME(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    op.create_table(
        'dataset',
        sa.Column('id', sa.INTEGER(), nullable=False),
        sa.Column('updated_at', sa.DATETIME(), nullable=True),
        sa.Column('created_at', sa.DATETIME(), nullable=True),
        sa.Column('name', sa.VARCHAR(), nullable=True),
        sa.Column('data', sa.VARCHAR(), nullable=True),
        sa.Column('creation_info', sa.VARCHAR(), nullable=True),
        sa.Column('company_id', sa.INTEGER(), nullable=True),
        sa.Column('mindsdb_version', sa.VARCHAR(), nullable=True),
        sa.Column('datasources_version', sa.VARCHAR(), nullable=True),
        sa.Column('integration_id', sa.INTEGER(), nullable=True),
        sa.Column('analysis_id', sa.INTEGER(), nullable=True),
        sa.Column('ds_class', sa.VARCHAR(), nullable=True),
        sa.ForeignKeyConstraint(['analysis_id'], ['analysis.id'], name='fk_analysis_id'),
        sa.ForeignKeyConstraint(['integration_id'], ['integration.id'], name='fk_integration_id'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name', 'company_id', name='unique_dataset_name_company_id')
    )

    with op.batch_alter_table('file', schema=None) as batch_op:
        batch_op.add_column(sa.Column('analysis_id', sa.INTEGER(), nullable=True))
        batch_op.create_foreign_key('fk_analysis_id', 'analysis', ['analysis_id'], ['id'])
        batch_op.create_unique_constraint('unique_file_name_company_id', ['name', 'company_id'])
