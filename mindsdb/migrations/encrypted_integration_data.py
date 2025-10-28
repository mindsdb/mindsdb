"""
Migration script to convert Integration.data column from Json to EncryptedJson type.

This migration handles the conversion of existing Integration data to support
optional KMS encryption. The migration is designed to be safe and reversible.

Usage:
    python -m mindsdb.migrations.encrypted_integration_data
"""

import os
import sys
from pathlib import Path

# Add the mindsdb package to the path
mindsdb_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(mindsdb_path))

from mindsdb.interfaces.storage.db import init, session, Integration
from mindsdb.utilities.config import config


def migrate_integration_data():
    """
    Migrate Integration.data column to support encryption.
    
    This function:
    1. Initializes the database connection
    2. Checks if KMS encryption is enabled
    3. If enabled, encrypts existing data
    4. If disabled, ensures data is stored as regular JSON
    """
    print("Starting Integration data migration...")
    
    # Initialize database connection
    init()
    
    # Get all integrations
    integrations = session.query(Integration).all()
    print(f"Found {len(integrations)} integrations to migrate")
    
    kms_config = config.get('kms', {})
    encryption_enabled = kms_config.get('enabled', False)
    
    if encryption_enabled:
        print("KMS encryption is enabled - data will be encrypted")
    else:
        print("KMS encryption is disabled - data will remain as regular JSON")
    
    migrated_count = 0
    
    for integration in integrations:
        try:
            # The EncryptedJson type will handle the conversion automatically
            # when we access and save the data
            if integration.data is not None:
                # Trigger the type conversion by accessing and setting the data
                data = integration.data
                integration.data = data
                migrated_count += 1
                
        except Exception as e:
            print(f"Error migrating integration {integration.name}: {e}")
            continue
    
    # Commit all changes
    try:
        session.commit()
        print(f"Successfully migrated {migrated_count} integrations")
    except Exception as e:
        session.rollback()
        print(f"Error committing migration: {e}")
        return False
    
    return True


def rollback_migration():
    """
    Rollback the migration by converting encrypted data back to regular JSON.
    
    Note: This function assumes KMS encryption is disabled during rollback.
    """
    print("Rolling back Integration data migration...")
    
    # Initialize database connection
    init()
    
    # Get all integrations
    integrations = session.query(Integration).all()
    print(f"Found {len(integrations)} integrations to rollback")
    
    kms_config = config.get('kms', {})
    encryption_enabled = kms_config.get('enabled', False)
    
    if encryption_enabled:
        print("Warning: KMS encryption is still enabled. Disable it first before rollback.")
        return False
    
    rolled_back_count = 0
    
    for integration in integrations:
        try:
            if integration.data is not None:
                # The EncryptedJson type will handle the conversion automatically
                data = integration.data
                integration.data = data
                rolled_back_count += 1
                
        except Exception as e:
            print(f"Error rolling back integration {integration.name}: {e}")
            continue
    
    # Commit all changes
    try:
        session.commit()
        print(f"Successfully rolled back {rolled_back_count} integrations")
    except Exception as e:
        session.rollback()
        print(f"Error committing rollback: {e}")
        return False
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate Integration data to support KMS encryption")
    parser.add_argument("--rollback", action="store_true", help="Rollback the migration")
    
    args = parser.parse_args()
    
    if args.rollback:
        success = rollback_migration()
    else:
        success = migrate_integration_data()
    
    sys.exit(0 if success else 1)
