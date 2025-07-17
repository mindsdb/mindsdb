"""added_metadata_to_projects

Revision ID: 11347c213b36
Revises: 4521dafe89ab
Create Date: 2025-02-19 18:46:24.014843

"""
from alembic import op
from sqlalchemy.orm.attributes import flag_modified
import sqlalchemy as sa

import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import config


# revision identifiers, used by Alembic.
revision = '11347c213b36'
down_revision = '4521dafe89ab'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('project', schema=None) as batch_op:
        batch_op.add_column(sa.Column('metadata', sa.JSON(), nullable=True))

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)
    session.commit()

    project = session.query(db.Project).filter_by(name='mindsdb').first()
    if project:
        project.name = config.get('default_project')
        project.metadata_ = {"is_default": True}
        flag_modified(project, 'metadata_')
        session.commit()


def downgrade():
    with op.batch_alter_table('project', schema=None) as batch_op:
        batch_op.drop_column('metadata')
