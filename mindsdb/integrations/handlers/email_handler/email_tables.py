import datetime as dt
import pytz

import pandas as pd

from mindsdb_sql_parser import ast

from mindsdb.integrations.handlers.email_handler.email_ingestor import EmailIngestor
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.utilities.handlers.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.handlers.email_handler.settings import EmailSearchOptions
from mindsdb.integrations.handlers.email_handler.email_client import _sanitize_mailbox
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class EmailsTable(APITable):
    """The Emails Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls email data from the connected account.
        """
        select_statement_parser = SELECTQueryParser(query, "emails", self.get_columns())
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}

        # Dispatch table for columns
        def handle_datetime(op, value):
            date = self.parse_date(value)
            if op == ">":
                search_params["since_date"] = date
            elif op == "<":
                search_params["until_date"] = date
            else:
                raise NotImplementedError("Only > and < operators are supported for created_at column.")

        def handle_id(op, value):
            if op not in ["=", ">", ">="]:
                raise NotImplementedError("Only =, > and >= operators are supported for id column.")
            if op in ["=", ">="]:
                search_params["since_email_id"] = int(value)
            elif op == ">":
                search_params["since_email_id"] = int(value) + 1

        def handle_eq_string(field_name, op, value):
            if op != "=":
                raise NotImplementedError("Only = operator is supported for mailbox, subject, to and from columns.")
            if field_name == "mailbox":
                search_params["mailbox"] = _sanitize_mailbox(str(value))
            else:
                search_params[field_name] = value

        handlers = {
            "datetime": lambda op, val: handle_datetime(op, val),
            "id": lambda op, val: handle_id(op, val),
            "mailbox": lambda op, val: handle_eq_string("mailbox", op, val),
            "subject": lambda op, val: handle_eq_string("subject", op, val),
            "to_field": lambda op, val: handle_eq_string("to_field", op, val),
            "from_field": lambda op, val: handle_eq_string("from_field", op, val),
        }

        for op, arg1, arg2 in where_conditions:
            if arg2 is None:
                logger.warning(
                    f"Skipping condition: {arg1} {op} {arg2}."
                    " Please ignore if this is intentional, e.g. 'id > last' on first query of job run."
                )
                continue
            handler = handlers.get(arg1)
            if handler is None:
                raise NotImplementedError(f"Unsupported column: {arg1}.")
            handler(op, arg2)

        self.handler.connect()

        # Enforce a hard upper bound of 1000 on results regardless of user-supplied LIMIT
        effective_limit = 1000
        if result_limit:
            try:
                effective_limit = min(int(result_limit), 1000)
            except Exception:
                effective_limit = 1000

        # Propagate LIMIT to search options to cap IMAP calls (client-side).
        search_params["max_results"] = effective_limit

        # Always sanitize mailbox even when not provided, so defaults fail fast if invalid
        if "mailbox" in search_params:
            search_params["mailbox"] = _sanitize_mailbox(str(search_params["mailbox"]))
        else:
            search_params["mailbox"] = _sanitize_mailbox("INBOX")

        search_options = EmailSearchOptions(**search_params) if search_params else EmailSearchOptions()

        email_ingestor = EmailIngestor(self.handler.connection, search_options)

        emails_df = email_ingestor.ingest()

        # Apply SELECT/ORDER/LIMIT on the DataFrame with the enforced cap
        select_statement_executor = SELECTQueryExecutor(
            emails_df, selected_columns, [], order_by_conditions, effective_limit
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """
        Sends emails through the connected account.
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=["to_field", "subject", "body"],
            mandatory_columns=["to_field", "subject", "body"],
            all_mandatory=True,
        )
        email_data = insert_statement_parser.parse_query()

        # Reuse a single connection for the batch to avoid connection thrashing
        connection = self.handler.connect()
        for email in email_data:
            to_addr = email["to_field"]
            payload = {k: v for k, v in email.items() if k != "to_field"}
            connection.send_email(to_addr, **payload)

    def get_columns(self):
        # Columns available for selection. Conditions are only supported on a subset handled in select().
        return [
            "id",
            "body_safe",
            "body_content_type",
            "subject",
            "to_field",
            "from_field",
            "datetime",
        ]

    @staticmethod
    def parse_date(date_str) -> dt.datetime:
        if isinstance(date_str, dt.datetime):
            return date_str
        date_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
        date = None
        for date_format in date_formats:
            try:
                date = dt.datetime.strptime(date_str, date_format)
            except ValueError:
                pass
        if date is None:
            raise ValueError(f"Can't parse date: {date_str}")
        date = date.astimezone(pytz.utc)
        return date
