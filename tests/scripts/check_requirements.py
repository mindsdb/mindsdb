import glob
import re
import sys
import subprocess
import os
import json

pattern = '\=|~|>|<| |\n|#|\['  # noqa: W605


def get_requirements_from_file(path):
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

HANDLER_REQS_PATHS = list(
    set(glob.glob("**/requirements*.txt", recursive=True))
    - set(glob.glob("requirements/requirements*.txt"))
)

MAIN_EXCLUDE_PATHS = ["mindsdb/integrations/handlers/.*_handler", "pryproject.toml"]

# Torch.multiprocessing is imported in a 'try'. Falls back to multiprocessing so we dont NEED it.
# Psycopg2 is needed in core codebase for sqlalchemy.
# lark is required for auto retrieval (RAG utilities). It is used by langchain
# and not explicitly imported in mindsdb.
# transformers is required for langchain_core and not explicitly imported by mindsdb.
MAIN_RULE_IGNORES = {
    "DEP003": ["torch"],
    "DEP001": ["torch", "pgvector"],
    "DEP002": ["psycopg2-binary", "lark", "transformers"]
}


# The following packages need exceptions.
# Either because 1) they are optional deps of some other packages. E.g.:
#   - langchain CAN use openai
#   - pypdf and openpyxl are optional deps of langchain, that are used for the file handler
# Or 2) because they are imported in an unusual way. E.g.:
#   - pysqlite3 in the chromadb handler
#   - dspy-ai in langchain handler
OPTIONAL_HANDLER_DEPS = ["torch", "tiktoken", "wikipedia", "openpyxl",
                         "sentence-transformers", "faiss-cpu", "litellm", "chromadb", "dspy-ai", "sqlalchemy-solr"]

# List of rules we can ignore for specific packages
# Here we ignore any packages in the main requirements.txt for "listed but not used" errors, because they will be used for the core code but not necessarily in a given handler
MAIN_REQUIREMENTS_DEPS = get_requirements_from_file(MAIN_REQS_PATH) + get_requirements_from_file(
    TEST_REQS_PATH)

BYOM_HANLDER_DEPS = ["pyarrow"]
# The `thrift-sasl` package is required establish a connection via to Hive via `pyhive`, but it is not explicitly imported in the code.
HIVE_HANDLER_DEPS = ["thrift-sasl"]

# The `gcsfs` package is required to interact with GCS as a file system.
GCS_HANDLER_DEPS = ["gcsfs"]

HANDLER_RULE_IGNORES = {
    "DEP002": OPTIONAL_HANDLER_DEPS + MAIN_REQUIREMENTS_DEPS + BYOM_HANLDER_DEPS + HIVE_HANDLER_DEPS + GCS_HANDLER_DEPS,
    "DEP001": ["tests"]  # 'tests' is the mindsdb tests folder in the repo root
}

PACKAGE_NAME_MAP = {
    "azure-storage-blob": ["azure"],
    "scylla-driver": ["cassandra"],
    "mysql-connector-python": ["mysql"],
    "snowflake-connector-python": ["snowflake"],
    "snowflake-sqlalchemy": ["snowflake"],
    "auto-sklearn": ["autosklearn"],
    "google-cloud-aiplatform": ["google"],
    "google-cloud-bigquery": ["google"],
    "google-cloud-spanner": ["google"],
    "sqlalchemy-spanner": ["google"],
    "google-auth-httplib2": ["google"],
    "google-generativeai": ["google"],
    "google-analytics-admin": ["google"],
    "google-auth": ["google"],
    "google-cloud-storage": ["google"],
    "protobuf": ["google"],
    "google-api-python-client": ["googleapiclient"],
    "ibm-cos-sdk": ["ibm_boto3", "ibm_botocore"],
    "binance-connector": ["binance"],
    "pysqlite3": ["pysqlite3"],
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
    "sqlalchemy-redshift": ["redshift_sqlalchemy"],
    "sqlalchemy-vertica-python": ["sqla_vertica_python"],
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
    "clickhouse-sqlalchemy": ["clickhouse_sqlalchemy"],
    "pillow": ["PIL"],
    "auto-ts": ["auto_ts"],
    "llama-index-readers-web": ["llama_index"],
    "llama-index-embeddings-openai": ["llama_index"],
    "unifyai": ["unify"],
    "botframework-connector": ["botframework"],
    "botbuilder-schema": ["botbuilder"],
    "opentelemetry-api": ["opentelemetry"],
    "opentelemetry-sdk": ["opentelemetry"],
    "opentelemetry-exporter-otlp": ["opentelemetry"],
    "opentelemetry-instrumentation-requests": ["opentelemetry"],
    "opentelemetry-instrumentation-flask": ["opentelemetry"],
}

# We use this to exit with a non-zero status code if any check fails
# so that when this is running in CI the job will fail
success = True


def print_errors(file, errors):
    global success
    if len(errors) > 0:
        success = False
        print(f"- {file}")
        for line in errors:
            print("    " + line)
        print()


def get_ignores_str(ignores_dict):
    """Get a list of rule ignores for deptry"""

    return ",".join([f"{k}={'|'.join(v)}" for k, v in ignores_dict.items()])


def run_deptry(reqs, rule_ignores, path, extra_args=""):
    """Run a dependency check with deptry. Return a list of error messages"""

    errors = []
    try:
        result = subprocess.run(
            f"deptry -o deptry.json --no-ansi --known-first-party mindsdb --requirements-files \"{reqs}\" --per-rule-ignores \"{rule_ignores}\" --package-module-name-map \"{get_ignores_str(PACKAGE_NAME_MAP)}\" {extra_args} {path}",
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        if result.returncode != 0 and not os.path.exists("deptry.json"):
            # There was some issue with running deptry
            errors.append(f"Error running deptry: {result.stderr.decode('utf-8')}")

        with open("deptry.json", "r") as f:
            deptry_results = json.loads(f.read())
        for r in deptry_results:
            errors.append(
                f"{r['location']['line']}:{r['location']['column']}: {r['error']['code']} {r['error']['message']}")
    finally:
        if os.path.exists("deptry.json"):
            os.remove("deptry.json")
    return errors


def check_for_requirements_duplicates():
    """Checks that handler requirements.txt and the main requirements.txt don't contain any of the same packages"""

    global success
    main_reqs = get_requirements_from_file(MAIN_REQS_PATH)

    for file in HANDLER_REQS_PATHS:
        handler_reqs = get_requirements_from_file(file)

        for req in handler_reqs:
            if req in main_reqs:
                print(f"{req} is in {file} and also in main requirements file.")
                success = False


def check_relative_reqs():
    """
    Check that relationships between handlers are defined correctly.

    If a parent handler imports another handler in code, we should define that dependency
    in the parent handler's requirements.txt like:

    -R mindsdb/integrations/handlers/child_handler/requirements.txt

    This is important to ensure that "pip install mindsdb[parent_handler]" works correctly.
    This function checks that for each handler imported from another handler, there is a
    corresponding entry in a requirements.txt.
    """

    global success
    # regex for finding relative imports of handlers like "from ..file_handler import FileHandler"
    # we're going to treat these as errors (and suggest using absolute imports instead)
    relative_import_pattern = re.compile("(?:\s|^)(?:from|import) \.\.\w+_handler")  # noqa: W605

    def get_relative_requirements(files):
        """Find entries in a requirements.txt that are including another requirements.txt"""
        entries = {}
        for file in files:
            with open(file, 'r') as fh:
                for line in fh.readlines():
                    line = line.lower().strip()
                    if line.startswith("-r mindsdb/integrations/handlers/"):
                        entries[line.split("mindsdb/integrations/handlers/")[1].split("/")[0]] = line

        return entries

    for handler_dir in glob.glob("mindsdb/integrations/handlers/*/"):
        handler_name = handler_dir.split("/")[-2].split("_handler")[0]

        # regex for finding imports of other handlers like "from mindsdb.integrations.handlers.file_handler import FileHandler"
        # excludes the current handler importing parts of itself
        import_pattern = re.compile(
            f"(?:\s|^)(?:from|import) mindsdb\.integrations\.handlers\.(?!{handler_name}_handler)\w+_handler")  # noqa: W605

        # requirements entries for this handler that point to another handler's requirements file
        required_handlers = get_relative_requirements(
            [file for file in HANDLER_REQS_PATHS if file.startswith(handler_dir)])

        all_imported_handlers = []

        # for every python file in this handler's code
        for file in glob.glob(f"{handler_dir}/**/*.py", recursive=True):
            errors = []

            # find all the imports of handlers
            with open(file, "r") as f:
                file_content = f.read()
                relative_imported_handlers = [match.strip() for match in
                                              re.findall(relative_import_pattern, file_content)]
                handler_import_lines = [match.strip() for match in re.findall(import_pattern, file_content)]

            imported_handlers = {line: line.split("_handler")[0].split(".")[-1] + "_handler" for line in
                                 handler_import_lines}
            all_imported_handlers += imported_handlers.values()

            # Report on relative imports (like "from ..file_handler import FileHandler")
            for line in relative_imported_handlers:
                errors.append(f"{line} <- Relative import of handler. Use absolute import instead")

            # Report on imports of other handlers that are missing a corresponding requirements.txt entry
            for line, imported_handler_name in imported_handlers.items():
                # Check if the imported handler has a requirements.txt file.
                imported_handler_req_file = f"mindsdb/integrations/handlers/{imported_handler_name}/requirements.txt"
                if os.path.exists(imported_handler_req_file):
                    if imported_handler_name not in required_handlers.keys():
                        errors.append(
                            f"{line} <- {imported_handler_name} not in handler requirements.txt. Add it like: \"-r {imported_handler_req_file}\"")

            # Print all the errors for this .py file
            print_errors(file, errors)

        # Report on requirements.txt entries that point to a handler that isn't used
        requirements_errors = [required_handler_name + " in requirements.txt but not used in code" for required_handler_name in required_handlers.keys() if
                               required_handler_name not in all_imported_handlers]
        print_errors(handler_dir, requirements_errors)

        # Report on requirements.txt entries that point to a handler requirements file that doesn't exist
        errors = []
        for _, required_handler_line in required_handlers.items():
            if not os.path.exists(required_handler_line.split('-r ')[1]):
                errors.append(f"{required_handler_line} <- this requirements file doesn't exist.")

        print_errors(handler_dir, errors)


def check_requirements_imports():
    """
    Use deptry to find issues with dependencies.

    Runs deptry on the core codebase (excluding handlers) + the main requirements.txt file.
    Then runs it on each handler codebase and requirements.txt individually.
    """

    # Run against the main codebase
    errors = run_deptry(
        ','.join([MAIN_REQS_PATH]),
        get_ignores_str(MAIN_RULE_IGNORES),
        ".",
        f"--extend-exclude \"{'|'.join(MAIN_EXCLUDE_PATHS)}\"",
    )
    print_errors(MAIN_REQS_PATH, errors)

    # Run on each handler
    for file in HANDLER_REQS_PATHS:
        errors = run_deptry(
            f"{file},{MAIN_REQS_PATH},{TEST_REQS_PATH}",
            get_ignores_str(HANDLER_RULE_IGNORES),
            os.path.dirname(file),
        )
        print_errors(file, errors)


print("--- Checking requirements files for duplicates ---")
check_for_requirements_duplicates()
print()

print("--- Checking that requirements match imports ---")
check_requirements_imports()
print()

print("--- Checking handlers that require other handlers ---")
check_relative_reqs()

sys.exit(0 if success else 1)
