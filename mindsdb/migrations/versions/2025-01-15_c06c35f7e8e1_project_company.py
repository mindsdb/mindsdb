"""project-company

Revision ID: c06c35f7e8e1
Revises: f6dc924079fa
Create Date: 2025-01-15 14:14:29.295834

"""
from collections import defaultdict

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa
from mindsdb.utilities import log

# revision identifiers, used by Alembic.
revision = 'c06c35f7e8e1'
down_revision = 'f6dc924079fa'
branch_labels = None
depends_on = None


logger = log.getLogger(__name__)


def upgrade():

    """
    convert company_id from null to 0 to make constrain works
    duplicated names are renamed
    """

    conn = op.get_bind()
    table = sa.Table(
        'project',
        sa.MetaData(),
        sa.Column('id', sa.Integer()),
        sa.Column('name', sa.String()),
        sa.Column('company_id', sa.Integer()),
    )

    data = conn.execute(
        table
        .select()
        .where(table.c.company_id == sa.null())
    ).fetchall()

    names = defaultdict(list)
    for id, name, _ in data:
        names[name].append(id)

    # get duplicated
    for name, ids in names.items():
        if len(ids) == 1:
            continue

        # rename all except first
        for id in ids[1:]:
            new_name = f'{name}__{id}'

            op.execute(
                table
                .update()
                .where(table.c.id == id)
                .values({'name': new_name})
            )
            logger.warning(f'Found duplicated project name: {name}, renamed to: {new_name}')

    op.execute(
        table
        .update()
        .where(table.c.company_id == sa.null())
        .values({'company_id': 0})
    )


def downgrade():
    table = sa.Table(
        'project',
        sa.MetaData(),
        sa.Column('company_id', sa.Integer())
    )

    op.execute(
        table
        .update()
        .where(table.c.company_id == 0)
        .values({'company_id': sa.null()})
    )
