from typing import Any, List, Optional
import uuid

import pandas as pd
import requests
from mindsdb_sql_parser import parse_sql

try:
    from requests_oauthlib import OAuth1
except ImportError:  # pragma: no cover - handled at runtime by __init__.py
    OAuth1 = None

from mindsdb.integrations.handlers.netsuite_handler.netsuite_tables import NetSuiteRecordTable
from mindsdb.integrations.handlers.netsuite_handler.__about__ import __version__ as handler_version
from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response, HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class NetSuiteHandler(MetaAPIHandler):
    """
    This handler manages connections and queries for the Oracle NetSuite REST APIs.
    """

    name = "netsuite"

    # Default record types to register when record_types is not provided.
    # NOTE: NetSuite metadata-catalog could be used to discover this list, but it can take 30-60 seconds to respond.
    # For now we stick with hardocded value
    DEFAULT_TABLES = [
        # --- Core reference / dimensions (joins everywhere) ---
        "account",
        "accountingperiod",
        "subsidiary",
        "currency",
        "currencyrate",
        "department",
        "classification",
        "term",
        "taxtype",
        "paymentmethod",
        "pricelevel",
        "pricebook",
        "priceplan",
        "pricinggroup",
        # --- CRM / entities ---
        "customer",
        "contact",
        "vendor",
        "employee",
        "partner",
        "opportunity",
        "supportcase",
        "campaign",
        "campaignresponse",
        # --- Sales / AR transactions ---
        "estimate",
        "salesorder",
        "invoice",
        "itemfulfillment",
        "customerpayment",
        "customerdeposit",
        "customerrefund",
        "creditmemo",
        "cashsale",
        "cashrefund",
        "returnauthorization",
        # --- Purchasing / AP transactions ---
        "purchaseorder",
        "purchaserequisition",
        "purchasecontract",
        "vendorbill",
        "vendorpayment",
        "vendorcredit",
        "vendorreturnauthorization",
        "vendorprepayment",
        "vendorprepaymentapplication",
        # --- Cash / banking ---
        "deposit",
        "depositapplication",
        "check",
        # --- Items / product catalog ---
        "inventoryitem",
        "assemblyitem",
        "kititem",
        "itemgroup",
        "markupitem",
        "discountitem",
        "subtotalitem",
        "descriptionitem",
        "downloaditem",
        "shipitem",
        "servicesaleitem",
        "servicepurchaseitem",
        "serviceresaleitem",
        "noninventorysaleitem",
        "noninventorypurchaseitem",
        "noninventoryresaleitem",
        "otherchargesaleitem",
        "otherchargepurchaseitem",
        "otherchargeresaleitem",
        # --- Inventory / warehouse ---
        "location",
        "bin",
        "bintransfer",
        "binworksheet",
        "inventoryadjustment",
        "inventorytransfer",
        "inventorycount",
        "inventorynumber",
        "itemreceipt",
        "inboundshipment",
        # --- Manufacturing / work orders ---
        "workorder",
        "workorderissue",
        "workordercompletion",
        "workorderclose",
        "bom",
        "bomrevision",
        "manufacturingrouting",
        "manufacturingoperationtask",
        "manufacturingcosttemplate",
        # --- Accounting / journals ---
        "journalentry",
        "intercompanyjournalentry",
        "advintercompanyjournalentry",
        "periodendjournal",
        "statisticaljournalentry",
        "consolidatedexchangerate",
        "fairvalueprice",
        # --- Billing / subscriptions / rev rec ---
        "billingaccount",
        "billingschedule",
        "billingrevenueevent",
        "subscription",
        "subscriptionplan",
        "subscriptionterm",
        "subscriptionline",
        "subscriptionchangeorder",
        "revrectemplate",
        "revrecschedule",
        # --- Misc “useful for ops/support” ---
        "task",
        "phonecall",
        "message",
        "emailtemplate",
        "notetype",
    ]
    ACCESSIBLE_TABLES = {"contact", "customer", "item", "message", "subsidiary", "task", "transaction"}

    def __init__(self, name: str, **kwargs):
        """
        Initializes the handler.

        Args:
            name (str): The name of the handler instance.
            **kwargs: Arbitrary keyword arguments including connection_data.
        """
        super().__init__(name)

        self.connection_data = kwargs.get("connection_data", {}) or {}
        self.kwargs = kwargs

        self.session: Optional[requests.Session] = None
        self.is_connected: bool = False
        self.base_url = self._build_base_url()
        self._record_types_source: str = "default"
        self.record_types = self._get_record_types()
        self._metadata_catalog: Optional[List[dict]] = None
        self._metadata_catalog_failed: bool = False
        self._record_metadata_cache: dict[str, dict] = {}
        self._unsupported_record_types: set[str] = set()
        self._accessible_record_types: Optional[set[str]] = None
        self._accessible_record_types_loaded: bool = False

        for record_type in self.record_types:
            self._register_table(record_type, NetSuiteRecordTable(self, record_type))

    def _get_metadata_catalog(self, table_names: Optional[List[str]] = None) -> List[dict]:
        """
        Retrieves NetSuite record metadata catalog.

        Args:
            table_names (Optional[List[str]]): Optional list of record types to fetch individually.

        Returns:
            List[dict]: Metadata entries for record types.
        """
        if table_names:
            metadata_list = []
            for table_name in table_names:
                record_metadata = self._get_record_metadata(table_name)
                if isinstance(record_metadata, dict):
                    entry = dict(record_metadata)
                    entry.setdefault("recordType", table_name)
                    entry.setdefault("name", table_name)
                    metadata_list.append(entry)
            return metadata_list

        if self._metadata_catalog is not None:
            return self._metadata_catalog

        try:
            payload = self._request("GET", "/services/rest/record/v1/metadata-catalog")
        except Exception as exc:
            logger.warning("Failed to fetch NetSuite metadata catalog: %s", exc)
            self._metadata_catalog = []
            self._metadata_catalog_failed = True
            return self._metadata_catalog

        if isinstance(payload, dict):
            items = (
                payload.get("items") or payload.get("records") or payload.get("data") or payload.get("results") or []
            )
        elif isinstance(payload, list):
            items = payload
        else:
            items = []

        self._metadata_catalog = items if isinstance(items, list) else []
        return self._metadata_catalog

    def _get_accessible_record_types(self) -> Optional[set[str]]:
        """
        Returns a cached set of accessible record types based on the metadata catalog.
        """
        if self._accessible_record_types_loaded:
            return self._accessible_record_types or None

        catalog = self._get_metadata_catalog()
        if not catalog:
            self._accessible_record_types_loaded = True
            self._accessible_record_types = set()
            return None

        record_types = set()
        for item in catalog:
            if not isinstance(item, dict):
                continue
            name = item.get("recordType") or item.get("name") or item.get("id")
            if name:
                record_types.add(str(name).lower())

        self._accessible_record_types_loaded = True
        self._accessible_record_types = record_types
        return record_types or None

    def _get_record_metadata(self, record_type: str) -> Optional[dict]:
        """
        Retrieves metadata for a specific NetSuite record type.

        Args:
            record_type (str): NetSuite record type.

        Returns:
            Optional[dict]: Metadata dictionary if available.
        """
        normalized = str(record_type).lower()
        if normalized in self._record_metadata_cache:
            return self._record_metadata_cache[normalized]

        try:
            payload = self._request("GET", f"/services/rest/record/v1/metadata-catalog/{normalized}")
        except Exception as exc:
            logger.warning("Failed to fetch NetSuite metadata for %s: %s", normalized, exc)
            return None

        if isinstance(payload, dict):
            self._record_metadata_cache[normalized] = payload
            return payload
        return None

    def _build_base_url(self) -> str:
        """
        Builds the REST base URL for NetSuite.

        Returns:
            str: The base URL for NetSuite REST endpoints.
        """
        rest_domain = self.connection_data.get("rest_domain")
        if rest_domain:
            return rest_domain.rstrip("/")

        account_id = self.connection_data.get("account_id")
        if not account_id:
            return ""

        # NetSuite REST domains use dashes, while realm/account ids often use underscores (e.g. 123456_SB1 -> 123456-sb1)
        host = str(account_id).lower().replace("_", "-")

        return f"https://{host}.suitetalk.api.netsuite.com"

    def _get_record_types(self) -> List[str]:
        """
        Resolves the record types to register as tables.

        - If connection_data.record_types is provided: use that (as before).
        - If not provided: use only ACCESSIBLE_TABLES (allowed tables) to avoid
          registering tables you can't query under current role.
        - DEFAULT_TABLES remains as reference for future / broader roles.
        """
        record_types = self.connection_data.get("record_types")

        print("******************************")
        print(record_types)
        print("******************************")

        # Explicit config always wins
        if isinstance(record_types, str):
            self._record_types_source = "config"
            return [value.strip().lower() for value in record_types.split(",") if value.strip()]

        if isinstance(record_types, list):
            self._record_types_source = "config"
            return [value.strip().lower() for value in record_types if isinstance(value, str) and value.strip()]

        # Default behavior (no record_types provided): register ONLY allowed tables
        self._record_types_source = "default_allowed"
        return sorted({name.strip().lower() for name in self.ACCESSIBLE_TABLES if name and name.strip()})

    def connect(self) -> requests.Session:
        """
        Creates an authenticated NetSuite session using token-based authentication.

        Returns:
            requests.Session: An authenticated session for NetSuite REST calls.
        """
        if self.is_connected and self.session is not None:
            return self.session

        if OAuth1 is None:
            raise ImportError("requests-oauthlib is required for the NetSuite handler.")

        account_id = self.connection_data.get("account_id")
        consumer_key = self.connection_data.get("consumer_key")
        consumer_secret = self.connection_data.get("consumer_secret")
        token_id = self.connection_data.get("token_id")
        token_secret = self.connection_data.get("token_secret")
        realm = str(account_id).upper()

        missing = [
            field
            for field, value in {
                "account_id": account_id,
                "consumer_key": consumer_key,
                "consumer_secret": consumer_secret,
                "token_id": token_id,
                "token_secret": token_secret,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"Missing required NetSuite credentials: {', '.join(missing)}")

        self.session = requests.Session()
        self.session.auth = OAuth1(
            consumer_key,
            consumer_secret,
            token_id,
            token_secret,
            realm=realm,
            signature_method="HMAC-SHA256",
            signature_type="auth_header",
        )

        self.is_connected = True
        return self.session

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the NetSuite connection.

        Returns:
            StatusResponse: Status and error information if the connection fails.
        """
        response = StatusResponse(False)
        try:
            self.connect()
            # Use SuiteQL to validate credentials quickly without relying on a specific record type.
            self._request("POST", "/services/rest/query/v1/suiteql", json={"q": "SELECT 1"})

            response.success = True
        except Exception as exc:  # broad catch to expose to UI
            logger.error("NetSuite connection failed: %s", exc)
            response.error_message = str(exc)
            self.is_connected = False

        return response

    def meta_get_tables(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (Optional[List[str]]): Optional list of table names.

        Returns:
            Response: A response object containing the table metadata.
        """
        allowed = set(self._tables.keys())
        accessible = self._get_accessible_record_types()
        if accessible is not None:
            allowed = allowed & accessible

        if self._unsupported_record_types:
            allowed = allowed - {name.lower() for name in self._unsupported_record_types}

        if table_names is not None:
            allowed = allowed & {name.lower() for name in table_names}

        df = pd.DataFrame()
        for table_name, table_class in self._tables.items():
            if table_name not in allowed:
                continue
            try:
                if hasattr(table_class, "meta_get_tables"):
                    table_metadata = table_class.meta_get_tables(table_name)
                    df = pd.concat([df, pd.DataFrame([table_metadata])], ignore_index=True)
            except Exception:
                logger.exception(f"Error retrieving metadata for table {table_name}:")

        if len(df.columns) == 0:
            df = pd.DataFrame(
                columns=[
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    "TABLE_SCHEMA",
                    "TABLE_DESCRIPTION",
                    "ROW_COUNT",
                ]
            )

        return Response(RESPONSE_TYPE.TABLE, df)

    def meta_get_columns(self, table_names: Optional[List[str]] = None, **kwargs) -> Response:
        """
        Retrieves column metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve column metadata.

        Returns:
            Response: A response object containing the column metadata.
        """
        allowed = set(self._tables.keys())
        accessible = self._get_accessible_record_types()
        if accessible is not None:
            allowed = allowed & accessible

        if self._unsupported_record_types:
            allowed = allowed - {name.lower() for name in self._unsupported_record_types}

        if table_names is not None:
            allowed = allowed & {name.lower() for name in table_names}

        df = pd.DataFrame()
        for table_name, table_class in self._tables.items():
            if table_name not in allowed:
                continue
            try:
                if hasattr(table_class, "meta_get_columns"):
                    column_metadata = table_class.meta_get_columns(table_name, **kwargs)
                    df = pd.concat([df, pd.DataFrame(column_metadata)], ignore_index=True)
            except Exception:
                logger.exception(f"Error retrieving column metadata for table {table_name}:")

        if len(df.columns) == 0:
            df = pd.DataFrame(
                columns=[
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "COLUMN_DESCRIPTION",
                    "IS_NULLABLE",
                    "COLUMN_DEFAULT",
                ]
            )

        return Response(RESPONSE_TYPE.TABLE, df)

    def native_query(self, query: Any) -> Response:
        """
        Executes SuiteQL using the NetSuite REST Query API.

        Args:
            query (Any): SuiteQL query string or AST.

        Returns:
            Response: A response containing the query results.
        """
        if not isinstance(query, str):
            ast = parse_sql(query)
            return self.query(ast)

        suiteql = query.strip()
        if suiteql.endswith(";"):
            suiteql = suiteql[:-1]

        payload = self._request("POST", "/services/rest/query/v1/suiteql", json={"q": suiteql})
        items = payload.get("items", []) if isinstance(payload, dict) else []
        columns_meta = []
        if isinstance(payload, dict):
            columns_meta = payload.get("columnMetadata") or payload.get("columns") or []

        df = pd.DataFrame()
        if items:
            first_item = items[0]
            if isinstance(first_item, dict) and "values" in first_item and columns_meta:
                columns = []
                for idx, col in enumerate(columns_meta):
                    if isinstance(col, dict):
                        columns.append(col.get("name") or col.get("label") or f"col_{idx}")
                    else:
                        columns.append(str(col))
                rows = [item.get("values", []) for item in items]
                if rows:
                    max_len = max(len(row) for row in rows)
                    if max_len > len(columns):
                        columns.extend([f"col_{idx}" for idx in range(len(columns), max_len)])
                    rows = [row[: len(columns)] + [None] * max(0, len(columns) - len(row)) for row in rows]

                deduped_columns = []
                seen = {}
                for name in columns:
                    count = seen.get(name, 0) + 1
                    seen[name] = count
                    deduped_columns.append(name if count == 1 else f"{name}_{count}")

                df = pd.DataFrame(rows, columns=deduped_columns)
            else:
                df = pd.DataFrame(items)

        return Response(RESPONSE_TYPE.TABLE, df)

    def _suiteql_select(
        self,
        table: str,
        where_sql: str = "",
        limit: Optional[int] = None,
        targets: Optional[List[str]] = None,
        order_by_sql: str = "",
    ) -> dict:
        """
        Executes a SuiteQL SELECT and returns raw payload dict.
        """
        select_cols = "*"
        if targets:
            select_cols = ", ".join(targets)

        limit_sql = ""
        if limit is not None:
            n = int(limit)
            if n < 0:
                n = 0
            limit_sql = f" FETCH FIRST {n} ROWS ONLY"

        sql = f"SELECT {select_cols} FROM {table}{where_sql}{order_by_sql}{limit_sql}"
        return self._request("POST", "/services/rest/query/v1/suiteql", json={"q": sql})

    def _request(self, method: str, path: str, **kwargs):
        """
        Performs an authenticated NetSuite REST request.

        Args:
            method (str): HTTP method name.
            path (str): Relative or absolute URL for the request.
            **kwargs: Additional request parameters.

        Returns:
            Any: Parsed JSON response when available.
        """
        self.connect()

        if path.startswith("http"):
            url = path
        else:
            if not self.base_url:
                raise ValueError("REST domain could not be derived; provide rest_domain or account_id.")
            url = f"{self.base_url}{path}"

        headers = kwargs.pop("headers", {})
        headers.setdefault("Accept", "application/json")
        if method.upper() != "GET":
            headers.setdefault("Content-Type", "application/json")
        headers.setdefault("Prefer", "transient")
        headers.setdefault("User-Agent", f"mindsdb-netsuite-handler/{handler_version}")
        correlation_id = headers.get("X-Request-Id") or str(uuid.uuid4())
        headers["X-Request-Id"] = correlation_id

        response = self.session.request(method, url, headers=headers, **kwargs)

        if not response.ok:
            error_message, log_message = self._format_netsuite_error(response)
            logger.error(
                "NetSuite API error %s for %s %s (request_id=%s): %s",
                response.status_code,
                method,
                url,
                correlation_id,
                log_message,
            )
            raise RuntimeError(error_message)

        if response.text:
            try:
                return response.json()
            except ValueError:
                return response.text
        return None

    @staticmethod
    def _format_netsuite_error(response: requests.Response) -> tuple[str, str]:
        """
        Formats NetSuite error responses into user-friendly messages with safe logging.

        Args:
            response (requests.Response): HTTP response from NetSuite.

        Returns:
            tuple[str, str]: User-facing error message and log-safe message.
        """
        status = response.status_code
        request_id = response.headers.get("x-ns-request-id") or response.headers.get("x-request-id")
        title = None
        detail = None
        raw_text = response.text or ""

        try:
            payload = response.json()
        except ValueError:
            payload = None

        if isinstance(payload, dict):
            title = payload.get("title")
            details = payload.get("o:errorDetails") or payload.get("errorDetails") or []
            if details and isinstance(details, list):
                first_detail = details[0]
                if isinstance(first_detail, dict):
                    detail = first_detail.get("detail")

        parts = [f"NetSuite API error {status}"]
        if title:
            parts.append(f"{title}")
        if detail:
            parts.append(f"{detail}")
        if request_id:
            parts.append(f"request_id={request_id}")
        error_message = ": ".join(parts)

        log_body = raw_text
        if len(log_body) > 500:
            log_body = f"{log_body[:500]}... [truncated]"
        log_message = log_body if log_body else error_message

        return error_message, log_message
