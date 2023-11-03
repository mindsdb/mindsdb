import sib_api_v3_sdk
import pandas as pd

from typing import List, Optional, Dict
from mindsdb.utilities.log import get_log
from mindsdb.integrations.libs.api_handler import APITable
logger = get_log("integrations.sendinblue_handler")
from mindsdb_sql.parser import ast
from sib_api_v3_sdk.rest import ApiException
from datetime import datetime

from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor, UPDATEQueryExecutor, UPDATEQueryParser


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
    
    
    def update(self, query: 'ast.Update') -> None:
        """
        Updates data in Sendinblue "PUT /emailCampaigns/{campaignId}" API endpoint.
        
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
        # this parse the UPDATE statement to extract the new values and the conditions for the update
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        email_campaigns_df = pd.json_normalize(self.get_email_campaigns())
        update_query_executor = UPDATEQueryExecutor(
            email_campaigns_df,
            where_conditions
        )
         # this retrieves the current list of email campaigns 
        email_campaigns_df = update_query_executor.execute_query()
        # this  extracts the IDs of the campaigns that have been updated
        campaign_ids = email_campaigns_df['id'].tolist()
        
        self.update_email_campaigns(campaign_ids, values_to_update)

    def update_email_campaigns(self, campaign_ids: List[int], values_to_update: Dict) -> None:
         # this establish a connection to Sendinblue API
        connection = self.handler.connect()
        email_campaigns_api_instance = sib_api_v3_sdk.EmailCampaignsApi(connection)

        for campaign_id in campaign_ids:
            try:
                email_campaigns_api_instance.update_email_campaign(campaign_id, values_to_update)
                logger.info(f'Email Campaign {campaign_id} updated')
            except ApiException as e:
                logger.error(f"Exception when calling EmailCampaignsApi->update_email_campaign: {e}\n") 
