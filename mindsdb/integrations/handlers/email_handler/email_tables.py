import datetime as dt
import pytz

import pandas as pd

from mindsdb_sql_parser import ast

from mindsdb.integrations.handlers.email_handler.email_ingestor import EmailIngestor
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.utilities.handlers.query_utilities.insert_query_utilities import INSERTQueryParser
from mindsdb.integrations.handlers.email_handler.settings import EmailSearchOptions
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class EmailsTable(APITable):
    """The Emails Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls email data from the connected account.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Emails matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'emails',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        for op, arg1, arg2 in where_conditions:
            if arg2 is None:
                logger.warning(f"Skipping condition: {arg1} {op} {arg2}."
                               "Please ignore if this is intentional, e.g. 'id > last' on first query of job run."
                               )
                continue

            if arg1 == 'datetime':
                date = self.parse_date(arg2)
                if op == '>':
                    search_params['since_date'] = date
                elif op == '<':
                    search_params['until_date'] = date
                else:
                    raise NotImplementedError("Only > and < operators are supported for created_at column.")
                continue

            elif arg1 == 'id':
                if op not in ['=', '>', '>=']:
                    raise NotImplementedError("Only =, > and >= operators are supported for id column.")
                if op in ['=', '>=']:
                    search_params['since_email_id'] = int(arg2)
                elif op == '>':
                    search_params['since_email_id'] = int(arg2) + 1

            elif arg1 in ['mailbox', 'subject', 'to_field', 'from_field']:
                if op != '=':
                    raise NotImplementedError("Only = operator is supported for mailbox, subject, to and from columns.")
                else:
                    if arg1 == 'from_field':
                        search_params['from_field'] = arg2
                    else:
                        search_params[arg1] = arg2

            else:
                raise NotImplementedError(f"Unsupported column: {arg1}.")

        self.handler.connect()

        if search_params:
            search_options = EmailSearchOptions(**search_params)
        else:
            search_options = EmailSearchOptions()

        email_ingestor = EmailIngestor(self.handler.connection, search_options)

        emails_df = email_ingestor.ingest()

        # ensure all queries from query are applied to the dataframe
        select_statement_executor = SELECTQueryExecutor(
            emails_df,
            selected_columns,
            [],
            order_by_conditions,
            result_limit
        )
        return select_statement_executor.execute_query()

    def insert(self, query: ast.Insert) -> None:
        """Sends emails through the connected account.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=['to_field', 'subject', 'body'],
            mandatory_columns=['to_field', 'subject', 'body'],
            all_mandatory=True
        )
        email_data = insert_statement_parser.parse_query()

        for email in email_data:
            connection = self.handler.connect()
            to_addr = email['to_field']
            del email['to_field']
            connection.send_email(to_addr, **email)

    def get_columns(self):
        return ['id', 'body', 'subject', 'to_field', 'from_field', 'datetime']

    @staticmethod
    def parse_date(date_str) -> dt.datetime:

        if isinstance(date_str, dt.datetime):

            return date_str
        date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
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
