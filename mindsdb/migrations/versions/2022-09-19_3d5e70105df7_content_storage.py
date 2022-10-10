"""content_storage

Revision ID: 3d5e70105df7
Revises: 87b2df2b83e1
Create Date: 2022-09-19 11:30:09.182435

"""
from pathlib import Path
import shutil

from mindsdb.utilities.config import Config


# revision identifiers, used by Alembic.
revision = '3d5e70105df7'
down_revision = '87b2df2b83e1'
branch_labels = None
depends_on = None


def upgrade():
    config = Config()
    is_cloud = config.get('cloud', False)
    if is_cloud is True:
        return

    storage_path = Path(config.paths['storage'])
    for item in storage_path.iterdir():
        if item.is_file() and item.name.startswith('predictor_'):
            original_name = item.name
            temp_file_path = item.parent / f'{original_name}_temp'
            item.replace(temp_file_path)
            item.mkdir(exist_ok=True)
            temp_file_path.replace(item / original_name)

    root_path = Path(config.paths['root'])
    if (root_path / 'datasources').is_dir():
        shutil.rmtree(root_path / 'datasources')
    if (root_path / 'integrations').is_dir():
        shutil.rmtree(root_path / 'integrations')
    if (root_path / 'predictors').is_dir():
        shutil.rmtree(root_path / 'predictors')


def downgrade():
    config = Config()
    is_cloud = config.get('cloud', False)
    if is_cloud is True:
        return

    storage_path = Path(config.paths['storage'])
    for item in storage_path.iterdir():
        if item.is_dir() and item.name.startswith('predictor_'):
            original_name = item.name
            if (item / original_name).is_file():
                temp_dir_path = item.parent / f'{original_name}_temp'
                item.replace(temp_dir_path)
                (temp_dir_path / original_name).replace(item.parent / original_name)
                shutil.rmtree(temp_dir_path)
