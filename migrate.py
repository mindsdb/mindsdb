from pathlib import Path

from alembic.command import upgrade
from alembic.config import Config

config_file = Path(__file__).parent.parent / 'alembic.ini'
config = Config(config_file)

upgrade(config=config, revision='head')
