
Execution alembic should perform in mindsdb/migrations directory
with adding mindsdb folder to python path. Example:

`cd mindsdb/migrations`

`env PYTHONPATH=../../ alembic upgrade head`

## Create migration

- alembic revision --autogenerate -m <migration_name>

Creating migration is required after changing database models during development process.

## Create 'checkpoint' migration

During the development of the app, more and more migrations are added. When running the app in a new environment, significant time is required to apply all migrations one-by-one. To avoid this, it's useful to create a new 'base' migration which:
 - completely creates the current state of the DB if migrations have never been applied before (app running in a new environment)
 - does nothing otherwise (app running in an existing environment)

To create new 'checkpoint' migration:
 - Create a new 'base' migration
 - Edit the beginning of the `upgrade` method to prevent it from running in existing environments (see previous 'checkpoint' migrations for examples)
 - Add statements at the end of the `upgrade` method to initialize required data (such as the default project)
 - Clear the body of the `downgrade` method
 - Set `down_revision` to the ID of the previous migration
 - Set the ID of the created 'checkpoint' migration to `current_checkpoint` in `migrate.py`

## Apply all migrations

Migrations are applying automatically at start of application
In case when database version is newer than backend then not perform migrations.

## Manual manage database version
- alembic upgrade +2
- alembic downgrade -1
- alembic upgrade <revision_name>
- and other alembic commands
