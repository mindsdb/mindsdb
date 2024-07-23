"""merge divergent branches

Revision ID: 459a4cd24933
Revises: cbedc4968d5d, bfc6f44f5bc9
Create Date: 2024-07-18 23:38:29.998117

"""
import mindsdb.interfaces.storage.db  # noqa


# revision identifiers, used by Alembic.
revision = '459a4cd24933'
down_revision = ('cbedc4968d5d', 'bfc6f44f5bc9')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
