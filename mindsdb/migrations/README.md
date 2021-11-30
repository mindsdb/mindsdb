
## Create migration 

- alembic revision --autogenerate -m <migration_name>

## Apply all migrations

- python -m mindsdb.migrate
- python -m mindsdb.migrate --config <user_config>

## Manage database version
- alembic upgrade +2
- alembic downgrade -1
- alembic upgrade <revision_name>
- and other alembic commands

