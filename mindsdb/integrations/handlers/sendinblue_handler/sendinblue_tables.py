import sib_api_v3_sdk
import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable

from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor


class EmailCampaignsTable(APITable):
    """The Sendinblue Email Campaigns Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Sendinblue "GET /emailCampaigns" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Sendinblue Email Campaigns matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'email_campaigns',
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        email_campaigns_df = pd.json_normalize(self.get_email_campaigns(limit=result_limit))

        select_statement_executor = SELECTQueryExecutor(
            email_campaigns_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        email_campaigns_df = select_statement_executor.execute_query()

        return email_campaigns_df

    def get_columns(self) -> List[str]:
        return pd.json_normalize(self.get_email_campaigns(limit=1)).columns.tolist()

    def get_email_campaigns(self, **kwargs):
        connection = self.handler.connect()
        email_campaigns_api_instance = sib_api_v3_sdk.EmailCampaignsApi(connection)
        email_campaigns = email_campaigns_api_instance.get_email_campaigns(**kwargs)
        return [email_campaign for  email_campaign in email_campaigns.campaigns]
    
   def update(self, query: ast.Update) -> None:
    """Updates data in the Sendinblue Email Campaigns Table.

    Parameters
    ----------
    query : ast.Update
        Given SQL UPDATE query

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the query contains an unsupported condition
    """
    update_statement_parser = UPDATEQueryParser(
        query,
        'email_campaigns',
        self.get_columns()
    )
    set_clause, where_conditions = update_statement_parser.parse_query()

    
    email_campaigns_df = pd.json_normalize(self.get_email_campaigns())

    campaigns_to_update = UPDATEQueryExecutor(
        email_campaigns_df,
        where_conditions
    ).execute_query()


    for index, row in campaigns_to_update.iterrows():
        campaign_id = row['id']
        update_data = {col: row[col] for col in set_clause.keys()}
        try:
            self.handler.connect().update_email_campaign(campaign_id, update_data)
            print(f"Campaign {campaign_id} updated successfully")
        except Exception as e:
            print(f"Failed to update campaign {campaign_id}: {str(e)}")
