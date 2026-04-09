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
    """Takes a requirements file path and extracts only the package names from it"""

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
    """Extract package names that have 'pinned by Snyk' comment from requirements file"""
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


# All requirements files that are NOT handler-specific (used to build the DEP002 ignore list
# for handler scans, so packages from core/agents/kb/dev/test extras aren't flagged).
ALL_NON_HANDLER_REQS_PATHS = glob.glob(str(REPO_ROOT / "requirements/requirements*.txt"))

HANDLER_REQS_PATHS = list(
    set(glob.glob(str(REPO_ROOT / "**/requirements*.txt"), recursive=True))
    - set(ALL_NON_HANDLER_REQS_PATHS)
    - set(glob.glob(str(REPO_ROOT / "build/**/requirements*.txt"), recursive=True))
)

# The following packages need exceptions for handler-level deptry runs.
# Either because 1) they are optional deps of some other packages. E.g.:
#   - langchain CAN use openai
#   - pypdf and openpyxl are optional deps of langchain, that are used for the file handler
# Or 2) because they are imported in an unusual way. E.g.:
#   - pysqlite3 in the chromadb handler
#   - dspy-ai in langchain handler

# The `pyarrow` package used for DataFrame serialization.
# It is not explicitly imported in the code and used as follows:
# modules.append('pyarrow==19.0.0')
BYOM_DEP002_IGNORE_HANLDER_DEPS = ["pyarrow", "scikit-learn"]

# The `thrift-sasl` package is required establish a connection via to Hive via `pyhive`, but it is not explicitly imported in the code.
HIVE_DEP002_IGNORE_HANDLER_DEPS = ["thrift-sasl"]

# The `gcsfs` package is required to interact with GCS as a file system.
GCS_DEP002_IGNORE_HANDLER_DEPS = ["gcsfs"]

LINDORM_DEP002_IGNORE_HANDLER_DEPS = ["protobuf"]

HUGGINGFACE_DEP002_IGNORE_HANDLER_DEPS = ["torch"]

RAG_DEP002_IGNORE_HANDLER_DEPS = ["sentence-transformers"]

SOLR_DEP002_IGNORE_HANDLER_DEPS = ["sqlalchemy-solr"]

OPENAI_DEP002_IGNORE_HANDLER_DEPS = ["tiktoken"]

CHROMADB_EP002_IGNORE_HANDLER_DEPS = ["onnxruntime"]

FRESHDESK_EP002_IGNORE_HANDLER_DEPS = ["python-freshdesk"]

# The `pyarrow` package is used only if it is installed.
# The handler can work without it.
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

# Packages from all non-handler requirements files are ignored for handler DEP002 checks,
# because they are used for the core code but not necessarily in a given handler.
MAIN_REQUIREMENTS_DEPS = sum([get_requirements_from_file(p) for p in ALL_NON_HANDLER_REQS_PATHS], [])

HANDLER_RULE_IGNORES = {
    "DEP002": DEP002_IGNORE_HANDLER_DEPS + MAIN_REQUIREMENTS_DEPS,
    "DEP001": [
        "tests",
        "pyarrow",
        "IfxPyDbi",
        "ingres_sa_dialect",
        "pyodbc",
        "freshdesk",
    ],  # 'tests' is the mindsdb tests folder in the repo root, 'pyarrow' used in snowflake handler
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


def get_ignores_str(ignores_dict: dict, dep002_ignore: list[str] | None = None) -> str:
    """Get a list of rule ignores for deptry

    Args:
        ignores_dict: A dictionary of rule ignores for deptry
        dep002_ignore: Additional list of packages to ignore for DEP002

    Returns:
        A string of rule ignores for deptry
    """

    rules = []
    for k, v in ignores_dict.items():
        rules.append(f"{k}={'|'.join(v)}")
        if k == "DEP002" and dep002_ignore:
            rules[-1] += "|" + "|".join(dep002_ignore)

    return ",".join(rules)


def run_deptry_with_pyproject(path: str) -> list[str]:
    """Run deptry using configuration from pyproject.toml. Return a list of error messages.

    All ignore rules, excludes, and package mappings are read from [tool.deptry] in
    pyproject.toml — no CLI flags for those are needed here.
    """
    errors = []
    deptry_path = os.path.join(os.path.dirname(sys.executable), "deptry")
    output_file = REPO_ROOT / "deptry.json"
    try:
        result = subprocess.run(
            [
                deptry_path,
                "--no-ansi",
                "--config",
                str(REPO_ROOT / "pyproject.toml"),
                "-o",
                str(output_file),
                path,
            ],
            shell=False,
            cwd=str(REPO_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        if not output_file.exists():
            if result.returncode != 0:
                errors.append(f"Error running deptry: {result.stderr.decode('utf-8')}")
            else:
                errors.append("Error running deptry: deptry.json was not generated.")
            return errors

        with open(output_file, "r") as f:
            deptry_results = json.loads(f.read())
        for r in deptry_results:
            errors.append(
                f"{r['location']['line']}:{r['location']['column']}: {r['error']['code']} {r['error']['message']}"
            )
    finally:
        if output_file.exists():
            output_file.unlink()
    return errors


def run_deptry_with_requirements(reqs, rule_ignores, path, extra_args=""):
    """Run a dependency check with deptry using requirements files. Return a list of error messages."""

    errors = []
    deptry_path = os.path.join(os.path.dirname(sys.executable), "deptry")
    try:
        result = subprocess.run(
            f'{deptry_path} -o deptry.json --no-ansi --known-first-party mindsdb --requirements-files "{reqs}" --per-rule-ignores "{rule_ignores}" --package-module-name-map "{get_ignores_str(PACKAGE_NAME_MAP)}" {extra_args} {path}',
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
    """Disabled for uv lock export model.

    requirements/requirements.txt is now generated from uv.lock and includes resolved
    transitive packages, so overlap with handler requirements is expected and no longer
    a useful signal.
    """
    return


def check_relative_reqs():
    """
    Check that relationships between handlers are defined correctly.

    If a parent handler imports another handler in code, we should define that dependency
    in the parent handler's requirements.txt like:

    -r mindsdb/integrations/handlers/child_handler/requirements.txt

    This is important to ensure that "pip install mindsdb[parent_handler]" works correctly.
    This function checks that for each handler imported from another handler, there is a
    corresponding entry in a requirements.txt.
    """

    # regex for finding relative imports of handlers like "from ..file_handler import FileHandler"
    # we're going to treat these as errors (and suggest using absolute imports instead)
    relative_import_pattern = re.compile(r"(?:\s|^)(?:from|import) \.\.\w+_handler")

    def get_relative_requirements(files):
        """Find entries in a requirements.txt that are including another requirements.txt"""
        entries = {}
        for file in files:
            with open(file, "r") as fh:
                for line in fh.readlines():
                    line = line.lower().strip()
                    if line.startswith("-r mindsdb/integrations/handlers/"):
                        entries[line.split("mindsdb/integrations/handlers/")[1].split("/")[0]] = line

        return entries

    for handler_dir in glob.glob(str(REPO_ROOT) + "/mindsdb/integrations/handlers/*/"):
        if not handler_dir.rstrip("/").endswith("_handler"):
            continue
        handler_name = handler_dir.split("/")[-2].split("_handler")[0]

        # regex for finding imports of other handlers like "from mindsdb.integrations.handlers.file_handler import FileHandler"
        # excludes the current handler importing parts of itself
        import_pattern = re.compile(
            rf"(?:\s|^)(?:from|import) mindsdb\.integrations\.handlers\.(?!{handler_name}_handler)\w+_handler"
        )

        # requirements entries for this handler that point to another handler's requirements file
        required_handlers = get_relative_requirements(
            [file for file in HANDLER_REQS_PATHS if file.startswith(handler_dir)]
        )

        all_imported_handlers = []

        # for every python file in this handler's code
        for file in glob.glob(f"{handler_dir}/**/*.py", recursive=True):
            errors = []

            # find all the imports of handlers
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

            # Report on relative imports (like "from ..file_handler import FileHandler")
            for line in relative_imported_handlers:
                errors.append(f"{line} <- Relative import of handler. Use absolute import instead")

            # Report on imports of other handlers that are missing a corresponding requirements.txt entry
            for line, imported_handler_name in imported_handlers.items():
                # Check if the imported handler has a requirements.txt file.
                imported_handler_req_file = repo_path(
                    "mindsdb", "integrations", "handlers", imported_handler_name, "requirements.txt"
                )
                if os.path.exists(imported_handler_req_file):
                    if imported_handler_name not in required_handlers.keys():
                        errors.append(
                            f'{line} <- {imported_handler_name} not in handler requirements.txt. Add it like: "-r mindsdb/integrations/handlers/{imported_handler_name}/requirements.txt"'
                        )

            # Print all the errors for this .py file
            print_errors(file, errors)

        # Report on requirements.txt entries that point to a handler that isn't used
        requirements_errors = [
            required_handler_name + " in requirements.txt but not used in code"
            for required_handler_name in required_handlers.keys()
            if required_handler_name not in all_imported_handlers
        ]
        print_errors(handler_dir, requirements_errors)

        # Report on requirements.txt entries that point to a handler requirements file that doesn't exist
        errors = []
        for _, required_handler_line in required_handlers.items():
            req_path = required_handler_line.split("-r ")[1]
            if not os.path.exists(REPO_ROOT / req_path):
                errors.append(f"{required_handler_line} <- this requirements file doesn't exist.")

        print_errors(handler_dir, errors)


def check_requirements_imports():
    """
    Use deptry to find issues with dependencies.

    Runs deptry on the core codebase using pyproject.toml as the source of truth
    (all ignore rules and package mappings are configured there under [tool.deptry]).
    Then runs it on each handler codebase and requirements.txt individually.
    """

    # Run against the main codebase — config comes entirely from pyproject.toml
    errors = run_deptry_with_pyproject(str(REPO_ROOT))
    print_errors("pyproject.toml", errors)

    # Run on each handler
    for file in HANDLER_REQS_PATHS:
        handler_no_check = get_requirements_with_DEP002(file)

        ignore_str = get_ignores_str(HANDLER_RULE_IGNORES, dep002_ignore=handler_no_check)

        errors = run_deptry_with_requirements(
            ",".join([file] + ALL_NON_HANDLER_REQS_PATHS),
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
