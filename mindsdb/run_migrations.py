import os
os.chdir(os.path.abspath(os.path.dirname(__file__)))
os.environ["FLASK_APP"] = "make_migrations.py"
os.system("flask db upgrade")
