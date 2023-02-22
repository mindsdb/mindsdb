
Execution alembic should perform in mindsdb/migrations directory 
with adding mindsdb folder to python path. Example:
- cd mindsdb/migrations
- env PYTHONPATH=../../ alembic upgrade head

## Create migration 

- alembic revision --autogenerate -m <migration_name>

Creating migration is required after changing database models during development process.

## Apply all migrations

Migrations are applying automatically at start of application 
In case when database version is newer than backend then not perform migrations.

## Manual manage database version
- alembic upgrade +2
- alembic downgrade -1
- alembic upgrade <revision_name>
- and other alembic commands

