#!/bin/bash
set -e

echo "Initializing databases..."

# Check required environment variables
if [[ -z "${POSTGRES_USER:-}" || -z "${POSTGRES_DB:-}" ]]; then
  echo "Error: POSTGRES_USER and POSTGRES_DB environment variables must be set." >&2
  exit 1
fi

# Log message for clarity
echo "Creating additional databases..."

# Function to create a database
create_database() {
  local db_name=$1
  echo "Creating database: $db_name"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE $db_name'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db_name')\gexec
EOSQL
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE kb'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'kb')\gexec
EOSQL
}

# Function to create a user with permissions
create_user() {
  local username=$1
  local userpassword=$2
  local db_name=$3
  echo "Creating user: $username with access to database: $db_name"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db_name" <<-EOSQL
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$username') THEN
        CREATE USER $username WITH PASSWORD '$userpassword';
      ELSE
        RAISE NOTICE 'User $username already exists. Skipping.';
      END IF;
    END
    \$\$;
    GRANT ALL PRIVILEGES ON DATABASE $db_name TO $username;
    GRANT ALL PRIVILEGES ON SCHEMA public TO $username;
    GRANT ALL ON SCHEMA public TO $username;
EOSQL

  # Do the same for the kb db
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "kb" <<-EOSQL
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$username') THEN
        CREATE USER $username WITH PASSWORD '$userpassword';
      ELSE
        RAISE NOTICE 'User $username already exists. Skipping.';
      END IF;
    END
    \$\$;
    GRANT ALL PRIVILEGES ON DATABASE kb TO $username;
    GRANT ALL PRIVILEGES ON SCHEMA public TO $username;
    GRANT ALL ON SCHEMA public TO $username;
    CREATE EXTENSION IF NOT EXISTS vector;
EOSQL
}

# Create databases
create_database "mindsdb"

# Create user with permissions in a database
create_user "mindsdb" "mindsdb" "mindsdb"

echo "Database and user creation completed successfully."
