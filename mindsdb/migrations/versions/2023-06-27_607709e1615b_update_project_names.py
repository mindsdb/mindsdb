"""update_project_names

Revision ID: 607709e1615b
Revises: 4c26ad04eeaa
Create Date: 2023-06-27 18:33:29.436607

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = '607709e1615b'
down_revision = 'b5bf593ba659'
branch_labels = None
depends_on = None


def upgrade():

    def _rename(table):
        conn = op.get_bind()

        data = conn.execute(
            table
            .select()
            .where(table.c.name.like("%.%"))
        ).fetchall()

        for row in data:
            name = row[0]
            name2 = name.replace('.', '_')

            op.execute(
                table
                .update()
                .where(table.c.name == name)
                .values({'name': name2})
            )

    projects = sa.Table(
        'project',
        sa.MetaData(),
        sa.Column('name', sa.String()),
    )

    _rename(projects)

    integrations = sa.Table(
        'integration',
        sa.MetaData(),
        sa.Column('name', sa.String()),
    )

    _rename(integrations)


def downgrade():
    pass
