"""predictor_index

Revision ID: 4b3c9d63e89c
Revises: c67822e96833
Create Date: 2023-12-25 20:50:08.275299

"""
from alembic import op
import sqlalchemy as sa  # noqa


# revision identifiers, used by Alembic.
revision = '4b3c9d63e89c'
down_revision = 'c67822e96833'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('agent_skills', schema=None) as batch_op:
        try:
            batch_op.create_foreign_key('agent_skills_agent_id_fk', 'agents', ['agent_id'], ['id'])
            batch_op.create_foreign_key('agent_skills_skill_id_fk', 'skills', ['skill_id'], ['id'])
        except Exception:
            pass

    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.create_index(
            'predictor_index',
            ['company_id', 'name', 'version', 'active', 'deleted_at'],
            unique=True
        )


def downgrade():
    with op.batch_alter_table('predictor', schema=None) as batch_op:
        batch_op.drop_index('predictor_index')

    with op.batch_alter_table('agent_skills', schema=None) as batch_op:
        try:
            batch_op.drop_constraint('agent_skills_agent_id_fk', type_='foreignkey')
            batch_op.drop_constraint('agent_skills_skill_id_fk', type_='foreignkey')
        except Exception:
            pass
