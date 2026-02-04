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
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response, HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class NetSuiteHandler(APIHandler):
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
        self.record_types = self._get_record_types()

        for record_type in self.record_types:
            self._register_table(record_type, NetSuiteRecordTable(self, record_type))

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

        Returns:
            List[str]: List of record types.
        """
        record_types = self.connection_data.get("record_types")
        if record_types is None:
            return self.DEFAULT_TABLES
        if isinstance(record_types, str):
            return [value.strip() for value in record_types.split(",") if value.strip()]
        if isinstance(record_types, list):
            return [value for value in record_types if isinstance(value, str) and value]
        return self.DEFAULT_TABLES

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
