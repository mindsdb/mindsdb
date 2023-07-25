import datetime as dt
import ast
import pytz

import pandas as pd

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.handlers.utilities.query_utilities.insert_query_utilities import INSERTQueryParser


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
            if arg1 == 'created_at':
                date = self.parse_date(arg2)
                if op == '>':
                    search_params['since_date'] = date
                elif op == '<':
                    search_params['until_date'] = date
                else:
                    raise NotImplementedError("Only > and < operators are supported for created_at column.")
                continue

            # TODO: what exactly is the since_emailid? Is it the id of the email?
            elif arg1 == 'id':
                if op == '>':
                    search_params['since_emailid'] = arg2
                else:
                    raise NotImplementedError("Only > operator is supported for id column.")
                continue

            # TODO: these arguments should only be supported with the = operator, right?
            elif arg1 in ['mailbox', 'subject', 'to', 'from']:
                if op != '=':
                    raise NotImplementedError("Only = operator is supported for mailbox, subject, to and from columns.")
                else:
                    if arg1 == 'from':
                        search_params['from_'] = arg2
                    else:
                        search_params[arg1] = arg2

            else:
                raise NotImplementedError(f"Unsupported column: {arg1}.")

        connection = self.handler.connect()
        emails_df = connection.search_email(**search_params)

        select_statement_executor = SELECTQueryExecutor(
            emails_df,
            selected_columns,
            [],
            order_by_conditions
        )
        emails_df = select_statement_executor.execute_query()

        return emails_df

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
            supported_columns=['to', 'subject', 'body'],
            mandatory_columns=['to', 'subject', 'body'],
            all_mandatory=True
        )
        email_data = insert_statement_parser.parse_query()

        for email in email_data:
            connection = self.handler.connect()
            to_addr = email['to']
            del email['to']
            connection.send_email(to_addr, **email)

    def get_columns(self):
        return ['id', 'created_at', 'to', 'from', 'subject', 'body']

    @staticmethod
    def parse_date(date_str):
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