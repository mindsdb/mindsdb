"""add user_id field and make company_id non-nullable

Revision ID: f64112749455
Revises: 86b172b78a5b
Create Date: 2026-01-12 14:52:20.431290

"""

import re
import shutil
from pathlib import Path

from alembic import op
import sqlalchemy as sa
import mindsdb.interfaces.storage.db  # noqa
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.utilities.constants import DEFAULT_COMPANY_ID, DEFAULT_USER_ID

logger = log.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "f64112749455"
down_revision = "86b172b78a5b"
branch_labels = None
depends_on = None

# Old company_id patterns that need to be migrated to DEFAULT_COMPANY_ID
# These patterns represent legacy company_id values that should now use the default
OLD_COMPANY_ID_PATTERNS = ["None", "0"]

# Resource groups that have folders on disk that may need renaming
RESOURCE_GROUPS_WITH_FOLDERS = ["file", "tab", "predictor", "integration", "system"]


def _get_new_folder_name(old_name: str) -> str:
    """Convert old folder name to new format with DEFAULT_COMPANY_ID and DEFAULT_USER_ID.

    Old formats (company_id was None or 0, no user_id):
        - {resource_group}_None_{id}
        - {resource_group}_0_{id}
        - {resource_group}_{company_id}_{id}  (real company_id, no user_id)

    New format:
        - {resource_group}_{company_id}_{user_id}_{id}

    Args:
        old_name: The old folder name

    Returns:
        The new folder name with company_id and user_id, or None if no change needed
    """
    # First, handle old patterns with None or 0 as company_id
    for pattern in OLD_COMPANY_ID_PATTERNS:
        # Match patterns like "file_None_123" or "tab_0_0"
        regex = rf"^([a-z]+)_{re.escape(pattern)}_(.+)$"
        match = re.match(regex, old_name)
        if match:
            resource_group = match.group(1)
            resource_id = match.group(2)
            return f"{resource_group}_{DEFAULT_COMPANY_ID}_{DEFAULT_USER_ID}_{resource_id}"

    # Then, handle old format with real company_id but no user_id
    # Match patterns like "file_9_123" (company_id_resource_id, where company_id is not a UUID)
    # but NOT patterns that already have user_id like "file_9_abc-def_123"
    regex = r"^([a-z]+)_([^_]+)_(\d+)$"
    match = re.match(regex, old_name)
    if match:
        resource_group = match.group(1)
        company_id = match.group(2)
        resource_id = match.group(3)
        # Skip if company_id looks like a UUID (already in new format or DEFAULT_COMPANY_ID)
        if len(company_id) == 36 and company_id.count("-") == 4:
            return None
        # Skip if this is an old pattern we already handled
        if company_id in OLD_COMPANY_ID_PATTERNS:
            return None
        return f"{resource_group}_{company_id}_{DEFAULT_USER_ID}_{resource_id}"

    return None


def _migrate_storage_folders():
    """Rename storage folders from old company_id patterns to DEFAULT_COMPANY_ID.

    This handles folders like:
        - file_None_123 -> file_{DEFAULT_COMPANY_ID}_123
        - tab_None_0 -> tab_{DEFAULT_COMPANY_ID}_0
        - predictor_0_456 -> predictor_{DEFAULT_COMPANY_ID}_456
    """
    try:
        config = Config()
        content_path = Path(config["paths"]["content"])

        if not content_path.exists():
            logger.info("Content path does not exist, skipping folder migration")
            return

        for resource_group in RESOURCE_GROUPS_WITH_FOLDERS:
            resource_group_path = content_path / resource_group

            if not resource_group_path.exists():
                continue

            for folder in resource_group_path.iterdir():
                if not folder.is_dir():
                    continue

                new_name = _get_new_folder_name(folder.name)
                if new_name is None:
                    continue

                new_path = resource_group_path / new_name

                if new_path.exists():
                    logger.warning(
                        "Target folder already exists, merging: %s -> %s",
                        folder,
                        new_path,
                    )
                    # Merge contents: copy files from old folder to new, then remove old
                    for item in folder.iterdir():
                        dest = new_path / item.name
                        if not dest.exists():
                            if item.is_dir():
                                shutil.copytree(item, dest)
                            else:
                                shutil.copy2(item, dest)
                    shutil.rmtree(folder)
                else:
                    logger.info("Renaming folder: %s -> %s", folder.name, new_name)
                    folder.rename(new_path)

    except Exception as e:
        logger.warning("Error migrating storage folders: %s", e)
        # Don't fail the migration if folder rename fails
        # The data is still accessible, just at the old path


def _migrate_file_paths():
    """Update file_path column in the file table to new format with company_id and user_id.

    Old formats:
        - file_None_123
        - file_0_123
        - file_{company_id}_123  (real company_id, no user_id)

    New format:
        - file_{company_id}_{user_id}_123
    """
    connection = op.get_bind()

    # Step 1: Update file_path for files with None or 0 company_id pattern
    # These get DEFAULT_COMPANY_ID and DEFAULT_USER_ID
    for pattern in OLD_COMPANY_ID_PATTERNS:
        # For each file record matching the old pattern, update to new format
        # We need to extract the file_id and construct the new path
        if connection.dialect.name == "sqlite":
            # SQLite: Use substr to extract file_id and construct new path
            old_prefix = f"file_{pattern}_"
            new_prefix = f"file_{DEFAULT_COMPANY_ID}_{DEFAULT_USER_ID}_"
            connection.execute(
                sa.text(
                    f"UPDATE file SET file_path = '{new_prefix}' || substr(file_path, {len(old_prefix) + 1}) "
                    f"WHERE file_path LIKE '{old_prefix}%'"
                )
            )
        else:
            # PostgreSQL/MySQL: Use CONCAT and SUBSTRING
            old_prefix = f"file_{pattern}_"
            new_prefix = f"file_{DEFAULT_COMPANY_ID}_{DEFAULT_USER_ID}_"
            connection.execute(
                sa.text(
                    f"UPDATE file SET file_path = CONCAT('{new_prefix}', SUBSTRING(file_path, {len(old_prefix) + 1})) "
                    f"WHERE file_path LIKE '{old_prefix}%'"
                )
            )

    # Step 2: Update file_path for files with real company_id but no user_id
    # These keep their company_id and get DEFAULT_USER_ID added
    # Pattern: file_{company_id}_{file_id} where company_id is NOT a UUID
    # We update these by inserting DEFAULT_USER_ID between company_id and file_id

    # First, get all file records that need updating (those without user_id in path)
    # A path without user_id looks like: file_{company_id}_{file_id}
    # A path with user_id looks like: file_{company_id}_{user_id}_{file_id}
    # We can identify old format by checking if file_path matches the expected new format

    # Get records where file_path doesn't contain DEFAULT_USER_ID or any other UUID-like user_id
    # This is tricky with just SQL, so we'll update based on the file record's company_id and id
    result = connection.execute(sa.text("SELECT id, company_id, file_path FROM file"))
    rows = result.fetchall()

    for row in rows:
        file_id, company_id, file_path = row
        expected_new_path = f"file_{company_id}_{DEFAULT_USER_ID}_{file_id}"

        # Skip if already in new format
        if file_path == expected_new_path:
            continue

        # Skip if file_path already has 4 parts (already has user_id)
        parts = file_path.split("_") if file_path else []
        if len(parts) >= 4:
            # Already has user_id in path, might need company_id update only
            # Check if it starts with file_{company_id}_{user_id}_
            if parts[0] == "file" and parts[1] == company_id:
                continue

        # Update to new format
        connection.execute(
            sa.text("UPDATE file SET file_path = :new_path WHERE id = :file_id"),
            {"new_path": expected_new_path, "file_id": file_id},
        )

    logger.info("Migrated file_path column to new format with user_id")


def _revert_file_paths():
    """Revert file_path column to old format without user_id (for downgrade).

    New format: file_{company_id}_{user_id}_{file_id}
    Old format: file_{company_id}_{file_id}

    For records with DEFAULT_COMPANY_ID, revert to file_None_{file_id}
    """
    connection = op.get_bind()

    # Get all file records and revert their paths
    result = connection.execute(sa.text("SELECT id, company_id, file_path FROM file"))
    rows = result.fetchall()

    for row in rows:
        file_id, company_id, file_path = row

        # Determine old format company_id
        if company_id == DEFAULT_COMPANY_ID:
            old_company_id = "None"
        else:
            old_company_id = company_id

        old_path = f"file_{old_company_id}_{file_id}"

        # Skip if already in old format
        if file_path == old_path:
            continue

        connection.execute(
            sa.text("UPDATE file SET file_path = :old_path WHERE id = :file_id"),
            {"old_path": old_path, "file_id": file_id},
        )

    logger.info("Reverted file_path column to old format without user_id")


def _revert_storage_folders():
    """Revert storage folders to old naming without user_id (for downgrade).

    New format: {resource_group}_{company_id}_{user_id}_{resource_id}
    Old format: {resource_group}_{company_id}_{resource_id}

    For folders with DEFAULT_COMPANY_ID, revert to {resource_group}_None_{resource_id}
    """
    try:
        config = Config()
        content_path = Path(config["paths"]["content"])

        if not content_path.exists():
            return

        for resource_group in RESOURCE_GROUPS_WITH_FOLDERS:
            resource_group_path = content_path / resource_group

            if not resource_group_path.exists():
                continue

            for folder in resource_group_path.iterdir():
                if not folder.is_dir():
                    continue

                # Match new format: {resource_group}_{company_id}_{user_id}_{resource_id}
                # where user_id is DEFAULT_USER_ID (UUID format)
                parts = folder.name.split("_")

                # Need at least 4 parts for new format with user_id
                if len(parts) < 4:
                    continue

                # Check if this looks like new format with user_id
                # Pattern: resource_group_company_id_user_id_resource_id
                # For UUIDs with dashes, they get split, so we need to handle that
                prefix_with_user = f"{resource_group}_{DEFAULT_COMPANY_ID}_{DEFAULT_USER_ID}_"
                if folder.name.startswith(prefix_with_user):
                    # Revert to old format with 'None' company_id
                    resource_id = folder.name[len(prefix_with_user) :]
                    old_name = f"{resource_group}_None_{resource_id}"
                    old_path = resource_group_path / old_name

                    if not old_path.exists():
                        logger.info("Reverting folder: %s -> %s", folder.name, old_name)
                        folder.rename(old_path)
                    continue

                # Check for real company_id with DEFAULT_USER_ID
                # Pattern: {resource_group}_{company_id}_{DEFAULT_USER_ID}_{resource_id}
                # This is harder to detect, try to match by finding DEFAULT_USER_ID in the name
                user_id_pattern = f"_{DEFAULT_USER_ID}_"
                if user_id_pattern in folder.name:
                    # Extract company_id and resource_id
                    before_user_id = folder.name.split(user_id_pattern)[0]
                    after_user_id = folder.name.split(user_id_pattern)[1]

                    # before_user_id should be "{resource_group}_{company_id}"
                    if before_user_id.startswith(f"{resource_group}_"):
                        company_id = before_user_id[len(f"{resource_group}_") :]
                        resource_id = after_user_id
                        old_name = f"{resource_group}_{company_id}_{resource_id}"
                        old_path = resource_group_path / old_name

                        if not old_path.exists():
                            logger.info("Reverting folder: %s -> %s", folder.name, old_name)
                            folder.rename(old_path)

    except Exception as e:
        logger.warning("Error reverting storage folders: %s", e)


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
    # Also handle 'None' string which may have been stored when Python None was converted to string
    for table_name in TABLES_WITH_USER_ID:
        op.execute(
            f"UPDATE {table_name} SET company_id = '{DEFAULT_COMPANY_ID}' WHERE company_id IS NULL OR company_id = '' OR company_id = '0' OR company_id = 'None'"
        )

    # Migrate file paths in the database before schema changes
    # This updates file_path column from patterns like 'file_None_123' to 'file_{DEFAULT_COMPANY_ID}_123'
    _migrate_file_paths()

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

    # Migrate storage folders on disk
    # This renames folders like 'file_None_123' to 'file_{DEFAULT_COMPANY_ID}_123'
    # and 'tab_None_0' to 'tab_{DEFAULT_COMPANY_ID}_0'
    _migrate_storage_folders()


def downgrade():
    # Revert storage folders first (before database changes)
    _revert_storage_folders()

    # Revert file paths in the database
    _revert_file_paths()

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

    # Set company_id back to legacy 'None' value for records that had DEFAULT_COMPANY_ID
    for table_name in TABLES_WITH_USER_ID:
        op.execute(f"UPDATE {table_name} SET company_id = '0' WHERE company_id = '{DEFAULT_COMPANY_ID}'")
