import os
from flask import Flask
from flask_migrate import Migrate

from .interfaces.storage.db import *

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['MINDSDB_DB_CON']
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

migrate = Migrate(app, db)
