"""llm_log_json_in_out

Revision ID: a8a3fac369e7
Revises: 0f89b523f346
Create Date: 2024-11-28 17:19:20.798803

"""
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table
import mindsdb.interfaces.storage.db  # noqa

# revision identifiers, used by Alembic.
revision = 'a8a3fac369e7'
down_revision = '0f89b523f346'
branch_labels = None
depends_on = None


def upgrade():
    llm_log_table = table(
        'llm_log',
        sa.Column('id', sa.Integer),
        sa.Column('input', sa.String),
        sa.Column('output', sa.String),
        sa.Column('input_json', sa.JSON),
        sa.Column('output_json', sa.JSON)
    )

    with op.batch_alter_table('llm_log', schema=None) as batch_op:
        batch_op.add_column(sa.Column('input_json', sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column('output_json', sa.JSON(), nullable=True))

    connection = op.get_bind()
    for row in connection.execute(llm_log_table.select()):
        try:
            input_json = json.loads(row.input)
        except Exception:
            input_json = None

        output_json = None
        try:
            if row.output is not None:
                output_json = [str(row.output)]
        except Exception:
            pass

        connection.execute(
            llm_log_table.update().where(
                llm_log_table.c.id == row.id
            ).values(input_json=input_json, output_json=output_json)
        )

    with op.batch_alter_table('llm_log', schema=None) as batch_op:
        batch_op.drop_column('input')
        batch_op.alter_column('input_json', new_column_name='input')
        batch_op.drop_column('output')
        batch_op.alter_column('output_json', new_column_name='output')


def downgrade():
    llm_log_table = table(
        'llm_log',
        sa.Column('id', sa.Integer),
        sa.Column('input', sa.JSON),
        sa.Column('output', sa.JSON),
        sa.Column('input_str', sa.String),
        sa.Column('output_str', sa.String)
    )

    with op.batch_alter_table('llm_log', schema=None) as batch_op:
        batch_op.add_column(sa.Column('input_str', sa.String(), nullable=True))
        batch_op.add_column(sa.Column('output_str', sa.String(), nullable=True))

    connection = op.get_bind()
    for row in connection.execute(llm_log_table.select()):
        input_str = None
        if row.input is not None:
            try:
                input_str = json.dumps(row.input)
            except Exception:
                pass

        output_str = None
        if isinstance(row.output, list):
            try:
                output_str = '\n'.join(row.output)
            except Exception:
                pass

        connection.execute(
            llm_log_table.update().where(
                llm_log_table.c.id == row.id
            ).values(input_str=input_str, output_str=output_str)
        )

    with op.batch_alter_table('llm_log', schema=None) as batch_op:
        batch_op.drop_column('input')
        batch_op.alter_column('input_str', new_column_name='input')
        batch_op.drop_column('output')
        batch_op.alter_column('output_str', new_column_name='output')
