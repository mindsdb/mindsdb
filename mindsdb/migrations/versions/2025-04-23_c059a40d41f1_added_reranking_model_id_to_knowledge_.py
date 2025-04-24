"""added_reranking_model_id_to_knowledge_bases

Revision ID: c059a40d41f1
Revises: fda503400e43
Create Date: 2025-04-23 19:20:25.966894

"""
from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa



# revision identifiers, used by Alembic.
revision = 'c059a40d41f1'
down_revision = 'fda503400e43'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('knowledge_base', schema=None) as batch_op:
        batch_op.add_column(sa.Column("reranking_model_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_knowledge_base_reranking_model_id",
            "predictor",
            ["reranking_model_id"],
            ["id"]
        )


def downgrade():
    with op.batch_alter_table('knowledge_base', schema=None) as batch_op:
        batch_op.drop_constraint("fk_knowledge_base_reranking_model_id", type_="foreignkey")
        batch_op.drop_column("reranking_model_id")
