import os
os.chdir(os.path.abspath(os.path.dirname(__file__)))
my_env = os.environ.copy()
my_env["FLASK_APP"] = "make_migrations.py"
os.system("flask db upgrade")
