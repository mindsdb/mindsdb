mkdir -p ~/.ssh/
echo "$DB_MACHINE_KEY" > ~/.ssh/db_machine
echo "$DATABASE_CREDENTIALS" > ~/.mindsdb_credentials.json

echo "Installing OpenSSH Client"
Add-WindowsCapability -Online -Name OpenSSH.Client*

pip install -r requirements_test.txt

$env:USE_EXTERNAL_DB_SERVER = "1"

echo "USE_EXTERNAL_DB_SERVER:"
Get-ChildItem Env:USE_EXTERNAL_DB_SERVER

# # MongoDB
# echo -e "\n===============\ntest MongoDB\n===============\n"
# python tests/integration_tests/flows/test_mongo.py

# # PostgreSQL
# echo -e "\n===============\ntest PostgreSQL\n===============\n"
# python tests/integration_tests/flows/test_postgres.py

# # MySQL
# echo -e "\n===============\ntest MySQL\n===============\n"
# python tests/integration_tests/flows/test_mysql.py

# # MariaDB
# echo -e "\n===============\ntest MariaDB\n===============\n"
# python tests/integration_tests/flows/test_mariadb.py

# # ClickHouse
# echo -e "\n===============\ntest ClickHouse\n===============\n"
# python tests/integration_tests/flows/test_clickhouse.py

# # Cutsom model
# echo -e "\n===============\ntest Cutsom model\n===============\n"
# python tests/integration_tests/flows/test_custom_model.py

# HTTP
echo "\n===============\ntest HTTP\n===============\n"
python tests\integration_tests\api\test_http.py

# # user flow 1
# echo -e "\n===============\ntest user flow 1\n===============\n"
# python tests/integration_tests/flows/test_user_flow_1.py

# # user flow 2
# echo -e "\n===============\ntest user flow 2\n===============\n"
# python tests/integration_tests/flows/test_user_flow_2.py

# # flow with mistakes
# echo -e "\n===============\nflow with mistakes\n===============\n"
# python tests/integration_tests/flows/test_mistakes.py
