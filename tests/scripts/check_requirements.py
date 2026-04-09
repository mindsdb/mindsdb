import glob
import json
import os
import re
import subprocess
import sys
from pathlib import Path

pattern = r"\=|~|>|<| |\n|#|\["

REPO_ROOT = Path(__file__).resolve().parents[2]


def repo_path(*parts) -> str:
    return str(REPO_ROOT.joinpath(*parts))


def get_requirements_from_file(path, with_snyk: bool = True):
    """Takes a requirements file path and extracts only the package names from it."""

    with open(path, "r") as main_f:
        reqs = []
        for line in main_f.readlines():
            if with_snyk is False and "pinned by Snyk to avoid a vulnerability" in line:
                continue
            parts = re.split(pattern, line)
            if parts and parts[0]:
                reqs.append(parts[0])
    return reqs


def get_requirements_with_DEP002(path):
    """Extract package names that have ignore comments from requirements file."""
    no_check_packages = []

    with open(path, "r") as f:
        for line in f.readlines():
            line = line.strip()
            if (
                line
                and not line.startswith("#")
                and ("pinned by Snyk to avoid a vulnerability" in line or "ignore-DEP002" in line)
            ):
                package_name = re.split(pattern, line)[0]
                if package_name:
                    no_check_packages.append(package_name)

    return no_check_packages


MAIN_REQS_PATH = repo_path("requirements", "requirements.txt")
DEV_REQS_PATH = repo_path("requirements", "requirements-dev.txt")
TEST_REQS_PATH = repo_path("requirements", "requirements-test.txt")

UTILITIES_REQS_PATHS = [
    repo_path("mindsdb", "integrations", "utilities", "handlers", "auth_utilities", "microsoft", "requirements.txt"),
    repo_path("mindsdb", "integrations", "utilities", "handlers", "auth_utilities", "google", "requirements.txt"),
]

EXTRA_REQS_PATHS = [
    repo_path("requirements", "requirements-agents.txt"),
    repo_path("requirements", "requirements-kb.txt"),
]

HANDLER_REQS_PATHS = list(
    set(glob.glob(str(REPO_ROOT / "**/requirements*.txt"), recursive=True))
    - set(glob.glob(str(REPO_ROOT / "requirements/requirements*.txt")))
)

MAIN_EXCLUDE_PATHS = ["mindsdb/integrations/handlers/.*_handler", "pyproject.toml"]

MAIN_RULE_IGNORES = {
    "DEP003": [
        "torch",
        "pyarrow",
        "langfuse",
        "dataprep_ml",
        "hierarchicalforecast",
    ],
    "DEP001": [
        "torch",
        "pgvector",
        "pyarrow",
        "openai",
        "dataprep_ml",
        "opentelemetry",
        "langfuse",
        "langchain_aws",
        "pyodbc",
        "sklearn",
        "hierarchicalforecast",
    ],
    "DEP002": [
        "psycopg2-binary",
        "lark",
        "transformers",
        "langchain-experimental",
        "lxml",
        "openpyxl",
        "xlrd",
        "onnxruntime",
        "litellm",
        "numba",
        "urllib3",
        "faiss-cpu",
        "pyopenssl",
    ],
}

BYOM_DEP002_IGNORE_HANLDER_DEPS = ["pyarrow", "scikit-learn"]
HIVE_DEP002_IGNORE_HANDLER_DEPS = ["thrift-sasl"]
GCS_DEP002_IGNORE_HANDLER_DEPS = ["gcsfs"]
LINDORM_DEP002_IGNORE_HANDLER_DEPS = ["protobuf"]
HUGGINGFACE_DEP002_IGNORE_HANDLER_DEPS = ["torch"]
RAG_DEP002_IGNORE_HANDLER_DEPS = ["sentence-transformers"]
SOLR_DEP002_IGNORE_HANDLER_DEPS = ["sqlalchemy-solr"]
OPENAI_DEP002_IGNORE_HANDLER_DEPS = ["tiktoken"]
CHROMADB_EP002_IGNORE_HANDLER_DEPS = ["onnxruntime"]
FRESHDESK_EP002_IGNORE_HANDLER_DEPS = ["python-freshdesk"]

SNOWFLAKE_DEP003_IGNORE_HANDLER_DEPS = ["pyarrow"]

DEP002_IGNORE_HANDLER_DEPS = list(
    set(
        BYOM_DEP002_IGNORE_HANLDER_DEPS
        + HIVE_DEP002_IGNORE_HANDLER_DEPS
        + GCS_DEP002_IGNORE_HANDLER_DEPS
        + LINDORM_DEP002_IGNORE_HANDLER_DEPS
        + HUGGINGFACE_DEP002_IGNORE_HANDLER_DEPS
        + RAG_DEP002_IGNORE_HANDLER_DEPS
        + SOLR_DEP002_IGNORE_HANDLER_DEPS
        + OPENAI_DEP002_IGNORE_HANDLER_DEPS
        + CHROMADB_EP002_IGNORE_HANDLER_DEPS
        + FRESHDESK_EP002_IGNORE_HANDLER_DEPS
    )
)

DEP003_IGNORE_HANDLER_DEPS = list(set(SNOWFLAKE_DEP003_IGNORE_HANDLER_DEPS))

MAIN_REQUIREMENTS_DEPS = get_requirements_from_file(MAIN_REQS_PATH) + get_requirements_from_file(TEST_REQS_PATH)

HANDLER_RULE_IGNORES = {
    "DEP002": DEP002_IGNORE_HANDLER_DEPS + MAIN_REQUIREMENTS_DEPS,
    "DEP001": [
        "tests",
        "pyarrow",
        "IfxPyDbi",
        "ingres_sa_dialect",
        "pyodbc",
        "freshdesk",
    ],
    "DEP003": DEP003_IGNORE_HANDLER_DEPS,
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
    "google-auth-oauthlib": ["google_auth_oauthlib"],
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
    "opentelemetry-distro": ["opentelemetry"],
    "sqlalchemy-ingres": ["ingres_sa_dialect"],
    "pyaml": ["yaml"],
    "pydantic_core": ["pydantic"],
    "python-dotenv": ["dotenv"],
    "pyjwt": ["jwt"],
    "sklearn": ["scikit-learn"],
    "ag2": ["autogen"],
}

success = True


def print_errors(file, errors):
    global success
    if len(errors) > 0:
        success = False
        print(f"- {file}")
        for line in errors:
            print("    " + line)
        print()


def get_ignores_str(ignores_dict: dict, dep002_ignore: list[str] | None = None) -> str:
    rules = []
    for k, v in ignores_dict.items():
        rules.append(f"{k}={'|'.join(v)}")
        if k == "DEP002" and dep002_ignore:
            rules[-1] += "|" + "|".join(dep002_ignore)

    return ",".join(rules)


def run_deptry_with_requirements(reqs, rule_ignores, path, extra_args=""):
    errors = []
    deptry_path = os.path.join(os.path.dirname(sys.executable), "deptry")
    try:
        result = subprocess.run(
            f"{deptry_path} -o deptry.json --no-ansi --known-first-party mindsdb "
            f'--requirements-files "{reqs}" '
            f'--per-rule-ignores "{rule_ignores}" '
            f'--package-module-name-map "{get_ignores_str(PACKAGE_NAME_MAP)}" '
            f"{extra_args} {path}",
            shell=True,
            cwd=str(REPO_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if not os.path.exists(REPO_ROOT / "deptry.json"):
            if result.returncode != 0:
                errors.append(f"Error running deptry: {result.stderr.decode('utf-8')}")
            else:
                errors.append("Error running deptry: deptry.json was not generated.")
            return errors

        with open(REPO_ROOT / "deptry.json", "r") as f:
            deptry_results = json.loads(f.read())
        for r in deptry_results:
            errors.append(
                f"{r['location']['line']}:{r['location']['column']}: {r['error']['code']} {r['error']['message']}"
            )
    finally:
        if os.path.exists(REPO_ROOT / "deptry.json"):
            os.remove(REPO_ROOT / "deptry.json")
    return errors


def run_deptry_with_pyproject(rule_ignores, path, extra_args=""):
    errors = []
    deptry_path = os.path.join(os.path.dirname(sys.executable), "deptry")
    try:
        result = subprocess.run(
            f"{deptry_path} -o deptry.json --no-ansi --known-first-party mindsdb "
            f'--per-rule-ignores "{rule_ignores}" '
            f'--package-module-name-map "{get_ignores_str(PACKAGE_NAME_MAP)}" '
            f"{extra_args} {path}",
            shell=True,
            cwd=str(REPO_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if not os.path.exists(REPO_ROOT / "deptry.json"):
            if result.returncode != 0:
                errors.append(f"Error running deptry: {result.stderr.decode('utf-8')}")
            else:
                errors.append("Error running deptry: deptry.json was not generated.")
            return errors

        with open(REPO_ROOT / "deptry.json", "r") as f:
            deptry_results = json.loads(f.read())
        for r in deptry_results:
            errors.append(
                f"{r['location']['line']}:{r['location']['column']}: {r['error']['code']} {r['error']['message']}"
            )
    finally:
        if os.path.exists(REPO_ROOT / "deptry.json"):
            os.remove(REPO_ROOT / "deptry.json")
    return errors


def check_for_requirements_duplicates():
    """Disabled for uv lock export model."""
    return


def check_relative_reqs():
    relative_import_pattern = re.compile(r"(?:\s|^)(?:from|import) \.\.\w+_handler")

    def get_relative_requirements(files):
        entries = {}
        for file in files:
            with open(file, "r") as fh:
                for line in fh.readlines():
                    line = line.lower().strip()
                    if line.startswith("-r mindsdb/integrations/handlers/"):
                        entries[line.split("mindsdb/integrations/handlers/")[1].split("/")[0]] = line
        return entries

    for handler_dir in glob.glob(str(REPO_ROOT) + "/mindsdb/integrations/handlers/*/"):
        handler_name = handler_dir.split("/")[-2].split("_handler")[0]
        import_pattern = re.compile(
            rf"(?:\s|^)(?:from|import) mindsdb\.integrations\.handlers\.(?!{handler_name}_handler)\w+_handler"
        )

        required_handlers = get_relative_requirements(
            [file for file in HANDLER_REQS_PATHS if file.startswith(handler_dir)]
        )

        all_imported_handlers = []

        for file in glob.glob(f"{handler_dir}/**/*.py", recursive=True):
            errors = []

            with open(file, "r") as f:
                file_content = f.read()
                relative_imported_handlers = [
                    match.strip() for match in re.findall(relative_import_pattern, file_content)
                ]
                handler_import_lines = [match.strip() for match in re.findall(import_pattern, file_content)]

            imported_handlers = {
                line: line.split("_handler")[0].split(".")[-1] + "_handler" for line in handler_import_lines
            }
            all_imported_handlers += imported_handlers.values()

            for line in relative_imported_handlers:
                errors.append(f"{line} <- Relative import of handler. Use absolute import instead")

            for line, imported_handler_name in imported_handlers.items():
                imported_handler_req_file = repo_path(
                    "mindsdb", "integrations", "handlers", imported_handler_name, "requirements.txt"
                )
                if os.path.exists(imported_handler_req_file):
                    if imported_handler_name not in required_handlers.keys():
                        errors.append(
                            f'{line} <- {imported_handler_name} not in handler requirements.txt. Add it like: "-r mindsdb/integrations/handlers/{imported_handler_name}/requirements.txt"'
                        )

            print_errors(file, errors)

        requirements_errors = [
            required_handler_name + " in requirements.txt but not used in code"
            for required_handler_name in required_handlers.keys()
            if required_handler_name not in all_imported_handlers
        ]
        print_errors(handler_dir, requirements_errors)

        errors = []
        for _, required_handler_line in required_handlers.items():
            req_path = required_handler_line.split("-r ")[1]
            if not os.path.exists(REPO_ROOT / req_path):
                errors.append(f"{required_handler_line} <- this requirements file doesn't exist.")

        print_errors(handler_dir, errors)


def check_requirements_imports():
    errors = run_deptry_with_pyproject(
        get_ignores_str(MAIN_RULE_IGNORES),
        str(REPO_ROOT),
        f'--extend-exclude "{"|".join(MAIN_EXCLUDE_PATHS)}"',
    )
    print_errors("pyproject.toml", errors)

    for file in HANDLER_REQS_PATHS:
        handler_no_check = get_requirements_with_DEP002(file)
        ignore_str = get_ignores_str(HANDLER_RULE_IGNORES, dep002_ignore=handler_no_check)

        errors = run_deptry_with_requirements(
            f"{file},{MAIN_REQS_PATH},{TEST_REQS_PATH}",
            ignore_str,
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
