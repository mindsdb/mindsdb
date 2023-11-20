import glob
import re
import sys
import subprocess
import os
import json

pattern = '\=|~|>|<| |\n|#|\['  # noqa: W605


def get_reqs_from_file(path):
    """Takes a requirements file path and extracts only the package names from it"""

    with open(path, 'r') as main_f:
        reqs = [
            re.split(pattern, line)[0]
            for line in main_f.readlines()
            if re.split(pattern, line)[0]
        ]
    return reqs


MAIN_REQS_PATH = "requirements/requirements.txt"
DEV_REQS_PATH = "requirements/requirements-dev.txt"
TEST_REQS_PATH = "requirements/requirements-test.txt"
DOCKER_REQS_PATH = "docker/handler_discovery/requirements.txt"

HANDLER_REQS_PATHS = list(
    set(glob.glob("**/requirements*.txt", recursive=True))
    - set(glob.glob("requirements/requirements*.txt"))
)

MAIN_EXCLUDE_PATHS = ["mindsdb/integrations/handlers", "pryproject.toml"]

MAIN_RULE_IGNORES = {
    "DEP003": ["torch"],
    "DEP001": ["torch"],
    "DEP002": ["psycopg2-binary"],
}  # torch.multiprocessing is imported in a 'try'. Falls back to multiprocessing so we dont NEED it. Psycopg2 is needed in core codebase for sqlalchemy.


# THe following packages need exceptions because they are optional deps of some other packages. e.g. langchain CAN use openai
# (pysqlite3-binary is imported in an unusual way in the chromadb handler and needs to be excluded too)
# pypdf and openpyxl are optional deps of langchain, that are used for the file handler
OPTIONAL_HANDLER_DEPS = ["pysqlite3-binary", "torch", "openai", "tiktoken", "wikipedia", "anthropic", "pypdf", "openpyxl"]

# List of rules we can ignore for specific packages
# Here we ignore any packages in the main requirements.txt for "listed but not used" errors, because they will be used for the core code but not necessarily in a given handler
MAIN_REQUIREMENTS_DEPS = get_reqs_from_file("requirements/requirements.txt") + get_reqs_from_file("requirements/requirements-test.txt")

BYOM_HANLDER_DEPS = ["pyarrow"]

HANDLER_RULE_IGNORES = {
    "DEP002": OPTIONAL_HANDLER_DEPS + MAIN_REQUIREMENTS_DEPS + BYOM_HANLDER_DEPS,
    "DEP001": ["tests"]  # 'tests' is the mindsdb tests folder in the repo root
}

PACKAGE_NAME_MAP = {
    "scylla-driver": ["cassandra"],
    "mysql-connector-python": ["mysql"],
    "snowflake-connector-python": ["snowflake"],
    "snowflake-sqlalchemy": ["snowflake"],
    "auto-sklearn": ["autosklearn"],
    "google-cloud-bigquery": ["google"],
    "google-cloud-spanner": ["google"],
    "google-auth-httplib2": ["google"],
    "google-generativeai": ["google"],
    "protobuf": ["google"],
    "google-api-python-client": ["googleapiclient"],
    "binance-connector": ["binance"],
    "pysqlite3-binary": ["pysqlite3"],
    "sqlalchemy-spanner": ["sqlalchemy"],
    "atlassian-python-api": ["atlassian"],
    "databricks-sql-connector": ["databricks"],
    "elasticsearch-dbapi": ["es"],
    "pygithub": ["github"],
    "python-gitlab": ["gitlab"],
    "impyla": ["impala"],
    "IfxPy": ["IfxPyDbi"],
    "salesforce-merlion": ["merlion"],
    "newsapi-python": ["newsapi"],
    "pinecone-client": ["pinecone"],
    "plaid-python": ["plaid"],
    "faiss-cpu": ["faiss"],
    "writerai": ["writer"],
    "rocketchat_API": ["rocketchat_API"],
    "ShopifyAPI": ["shopify"],
    "solace-pubsubplus": ["solace"],
    "taospy": ["taosrest"],
    "weaviate-client": ["weaviate"],
    "pymupdf": ["fitz"],
    "ibm-db": ["ibm_db_dbi"],
    "python-dateutil": ["dateutil"],
    "grpcio": ["grpc"],
    "sqlalchemy-redshift": ["redshift_sqlalchemy"],
    "sqlalchemy-vertica-python": ["sqla_vertica_python"],
    "grpcio-tools": ["grpc"],
    "psycopg2-binary": ["psycopg2"],
    "psycopg-binary": ["psycopg"],
    "pymongo": ["pymongo", "bson"],
    "python-multipart": ["multipart"],
    "pydateinfer": ["dateinfer"],
    "scikit-learn": ["sklearn"],
    "influxdb3-python": ["influxdb_client_3"],
    "hubspot-api-client": ["hubspot"],
    "pytest-lazy-fixture": ["pytest_lazyfixture"],
    "eventbrite-python": ["eventbrite"],
    "python-magic": ["magic"],
    "clickhouse-sqlalchemy": ["clickhouse_sqlalchemy"],
    "pillow": ["PIL"],
}


success = True


def get_ignores_str(ignores_dict):
    return ",".join([f"{k}={'|'.join(v)}" for k, v in ignores_dict.items()])


def run_deptry(reqs, rule_ignores, path, extra_args=""):
    errors = []
    try:
        result = subprocess.run(
            f"deptry -o deptry.json --no-ansi --known-first-party mindsdb --requirements-txt \"{reqs}\" --per-rule-ignores \"{rule_ignores}\" --package-module-name-map \"{get_ignores_str(PACKAGE_NAME_MAP)}\" {extra_args} {path}",
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        if result.returncode != 0 and not os.path.exists("deptry.json"):
            errors.append(f"Error running deptry: {result.stderr.decode('utf-8')}")
        with open("deptry.json", "r") as f:
            deptry_results = json.loads(f.read())

        for r in deptry_results:
            errors.append(f"{r['location']['line']}:{r['location']['column']}: {r['error']['code']} {r['error']['message']}")
    finally:
        if os.path.exists("deptry.json"):
            os.remove("deptry.json")
        return errors


def check_for_requirements_duplicates():
    """Checks that handler requirements.txt and the main requirements.txt dont contain any of the same packages"""

    global success
    main_reqs = get_reqs_from_file(MAIN_REQS_PATH)

    for file in HANDLER_REQS_PATHS:
        handler_reqs = get_reqs_from_file(file)

        for req in handler_reqs:
            if req in main_reqs:
                print(f"{req} is in {file} and also in main reqs")
                success = False


def check_requirements_imports():

    global success
    errors = run_deptry(
        ','.join([MAIN_REQS_PATH, DOCKER_REQS_PATH]),
        get_ignores_str(MAIN_RULE_IGNORES),
        ".",
        f"--extend-exclude \"{'|'.join(MAIN_EXCLUDE_PATHS)}\"",
    )
    if len(errors) > 0:
        success = False
        print(f"- {MAIN_REQS_PATH}")
        for line in errors:
            print(line)
        print()

    for file in HANDLER_REQS_PATHS:
        errors = run_deptry(
            f"{file},{MAIN_REQS_PATH},{TEST_REQS_PATH}",
            get_ignores_str(HANDLER_RULE_IGNORES),
            os.path.dirname(file),
        )
        if len(errors) > 0:
            success = False
            print(f"- {file}")
            for line in errors:
                print(line)
            print()


print("--- Checking requirements files for duplicates ---")
check_for_requirements_duplicates()
print()

print("--- Checking that requirements match imports ---")
check_requirements_imports()


sys.exit(0 if success else 1)
