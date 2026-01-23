"""add user_id field and make company_id non-nullable

Revision ID: f64112749455
Revises: 86b172b78a5b
Create Date: 2026-01-12 14:52:20.431290

"""

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa
from mindsdb.utilities import log
from mindsdb.utilities.constants import DEFAULT_COMPANY_ID, DEFAULT_USER_ID

logger = log.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "f64112749455"
down_revision = "86b172b78a5b"
branch_labels = None
depends_on = None


# Tables that need user_id column added (considering only the tables that already have company_id column)
TABLES_WITH_USER_ID = [
    "predictor",
    "project",
    "integration",
    "file",
    "view",
    "json_storage",
    "jobs",
    "jobs_history",
    "tasks",
    "agents",
    "query_context",
    "queries",
    "llm_log",
]

# Old unique constraints to drop (name -> table) (considering only the constraints that already have company_id column)
OLD_CONSTRAINTS = {
    "unique_project_name_company_id": "project",
    "unique_integration_name_company_id": "integration",
    "unique_file_name_company_id": "file",
    "unique_view_name_company_id": "view",
}

# New unique constraints to create (name -> (table, columns)) (considering only the constraints that already have company_id column)
NEW_CONSTRAINTS = {
    "unique_project_name_company_id_user_id": ("project", ["name", "company_id", "user_id"]),
    "unique_integration_name_company_id_user_id": ("integration", ["name", "company_id", "user_id"]),
    "unique_file_name_company_id_user_id": ("file", ["name", "company_id", "user_id"]),
    "unique_view_name_company_id_user_id": ("view", ["name", "company_id", "user_id"]),
}


def _is_sqlite():
    """Check if the current database is SQLite."""
    bind = op.get_bind()
    return bind.dialect.name == "sqlite"


def upgrade():
    # First, update any NULL, empty, or legacy '0' company_id values to DEFAULT_COMPANY_ID before making the column non-nullable
    # Note: '0' was the legacy integer value that got converted to string by a previous migration
    for table_name in TABLES_WITH_USER_ID:
        op.execute(
            f"UPDATE {table_name} SET company_id = '{DEFAULT_COMPANY_ID}' WHERE company_id IS NULL OR company_id = '' OR company_id = '0'"
        )

    # Add user_id column and make company_id non-nullable with default DEFAULT_COMPANY_ID for all tables
    for table_name in TABLES_WITH_USER_ID:
        with op.batch_alter_table(table_name, schema=None) as batch_op:
            batch_op.add_column(sa.Column("user_id", sa.String(), nullable=False, server_default=DEFAULT_USER_ID))
            # Make company_id non-nullable with default DEFAULT_COMPANY_ID
            batch_op.alter_column(
                "company_id", existing_type=sa.String(), nullable=False, server_default=DEFAULT_COMPANY_ID
            )

    # Drop old unique constraints and create new ones with user_id
    # For SQLite, we need to use batch_alter_table which recreates the table
    # For PostgreSQL/MySQL, we can use DROP CONSTRAINT directly
    if _is_sqlite():
        # SQLite: Use batch_alter_table to drop and recreate constraints
        # batch_alter_table handles this by recreating the table without the constraint
        for constraint_name, table_name in OLD_CONSTRAINTS.items():
            try:
                with op.batch_alter_table(table_name, schema=None) as batch_op:
                    batch_op.drop_constraint(constraint_name, type_="unique")
            except Exception:
                # Constraint might not exist or have a different name in SQLite
                logger.warning(f"Could not drop constraint {constraint_name} from table {table_name}, it may not exist")
    else:
        # PostgreSQL/MySQL: Use standard SQL
        for constraint_name, table_name in OLD_CONSTRAINTS.items():
            try:
                op.drop_constraint(constraint_name, table_name, type_="unique")
            except Exception:
                logger.warning(f"Could not drop constraint {constraint_name} from table {table_name}, it may not exist")

    # Create new constraints with user_id
    for constraint_name, (table_name, columns) in NEW_CONSTRAINTS.items():
        try:
            with op.batch_alter_table(table_name, schema=None) as batch_op:
                batch_op.create_unique_constraint(constraint_name, columns)
        except Exception:
            logger.exception(f"Failed to create constraint {constraint_name} for table {table_name}")
            raise

    # Update predictor_index to include user_id
    with op.batch_alter_table("predictor", schema=None) as batch_op:
        try:
            batch_op.drop_index("predictor_index")
        except Exception:
            logger.exception("Failed to drop index predictor_index from table predictor")
            raise

        batch_op.create_index(
            "predictor_index", ["company_id", "user_id", "name", "version", "active", "deleted_at"], unique=True
        )


def downgrade():
    # Restore original predictor_index without user_id
    with op.batch_alter_table("predictor", schema=None) as batch_op:
        try:
            batch_op.drop_index("predictor_index")
        except Exception:
            logger.exception("Failed to drop index predictor_index from table predictor")
            raise

        batch_op.create_index("predictor_index", ["company_id", "name", "version", "active", "deleted_at"], unique=True)

    # Drop new unique constraints and restore old ones
    if _is_sqlite():
        for constraint_name, (table_name, _) in NEW_CONSTRAINTS.items():
            try:
                with op.batch_alter_table(table_name, schema=None) as batch_op:
                    batch_op.drop_constraint(constraint_name, type_="unique")
            except Exception:
                logger.warning(f"Could not drop constraint {constraint_name} from table {table_name}, it may not exist")
    else:
        for constraint_name, (table_name, _) in NEW_CONSTRAINTS.items():
            try:
                op.drop_constraint(constraint_name, table_name, type_="unique")
            except Exception:
                logger.warning(f"Could not drop constraint {constraint_name} from table {table_name}, it may not exist")

    # Restore old constraints without user_id
    old_constraint_columns = {
        "unique_project_name_company_id": ("project", ["name", "company_id"]),
        "unique_integration_name_company_id": ("integration", ["name", "company_id"]),
        "unique_file_name_company_id": ("file", ["name", "company_id"]),
        "unique_view_name_company_id": ("view", ["name", "company_id"]),
    }

    for constraint_name, (table_name, columns) in old_constraint_columns.items():
        try:
            with op.batch_alter_table(table_name, schema=None) as batch_op:
                batch_op.create_unique_constraint(constraint_name, columns)
        except Exception:
            logger.exception(f"Failed to create constraint {constraint_name} for table {table_name}")
            raise

    # Remove user_id column and revert company_id to nullable for all tables
    for table_name in TABLES_WITH_USER_ID:
        with op.batch_alter_table(table_name, schema=None) as batch_op:
            batch_op.drop_column("user_id")
            # Revert company_id to nullable without default
            batch_op.alter_column("company_id", existing_type=sa.String(), nullable=True, server_default=None)
