import sib_api_v3_sdk
import pandas as pd


from typing import List, Dict, Text, Any
from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb_sql_parser import ast
from sib_api_v3_sdk.rest import ApiException


from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
    UPDATEQueryExecutor,
    UPDATEQueryParser,
    DELETEQueryParser,
    DELETEQueryExecutor,
    INSERTQueryParser,
)

logger = log.getLogger(__name__)


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
            query, 'email_campaigns', self.get_columns()
        )
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        email_campaigns_df = pd.json_normalize(
            self.get_email_campaigns(limit=result_limit)
        )

        select_statement_executor = SELECTQueryExecutor(
            email_campaigns_df, selected_columns, where_conditions, order_by_conditions
        )
        email_campaigns_df = select_statement_executor.execute_query()

        return email_campaigns_df

    def get_columns(self) -> List[str]:
        return pd.json_normalize(self.get_email_campaigns(limit=1)).columns.tolist()

    def get_email_campaigns(self, **kwargs):
        connection = self.handler.connect()
        email_campaigns_api_instance = sib_api_v3_sdk.EmailCampaignsApi(connection)
        email_campaigns = email_campaigns_api_instance.get_email_campaigns(**kwargs)
        return [email_campaign for email_campaign in email_campaigns.campaigns]

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes an email campaign from Sendinblue.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        RuntimeError
            If an error occurs when calling Sendinblue's API
        """
        # this  parses the DELETE statement to extract where conditions
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()
        # this retrieves the current list of email campaigns and normalize the data into a DataFrame
        email_campaigns_df = pd.json_normalize(self.get_email_campaigns())
        # this execute the delete query  to filter out the campaigns to be deleted
        delete_query_executor = DELETEQueryExecutor(
            email_campaigns_df, where_conditions
        )
        # this gets the updated DataFrame after executing  delete conditions
        email_campaigns_df = delete_query_executor.execute_query()
        campaign_ids = email_campaigns_df['id'].tolist()
        self.delete_email_campaigns(campaign_ids)

    def delete_email_campaigns(self, campaign_ids: List[Text]) -> None:
        # this establish a connection to Sendinblue API
        connection = self.handler.connect()
        email_campaigns_api_instance = sib_api_v3_sdk.EmailCampaignsApi(connection)
        for campaign_id in campaign_ids:
            try:
                email_campaigns_api_instance.delete_email_campaign(campaign_id)
                logger.info(f'Email Campaign {campaign_id} deleted')
            except ApiException as e:
                logger.error(
                    f"Exception when calling EmailCampaignsApi->delete_email_campaign: {e}\n"
                )
                raise RuntimeError(
                    f"Failed to execute the delete command for Email Campaign {campaign_id}"
                ) from e

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
        RuntimeError
            If an error occurs when calling Sendinblue's API
        """
        # this parse the UPDATE statement to extract the new values and the conditions for the update
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        email_campaigns_df = pd.json_normalize(self.get_email_campaigns())
        update_query_executor = UPDATEQueryExecutor(
            email_campaigns_df, where_conditions
        )
        # this retrieves the current list of email campaigns
        email_campaigns_df = update_query_executor.execute_query()
        # this  extracts the IDs of the campaigns that have been updated
        campaign_ids = email_campaigns_df['id'].tolist()

        self.update_email_campaigns(campaign_ids, values_to_update)

    def update_email_campaigns(
        self, campaign_ids: List[int], values_to_update: Dict
    ) -> None:
        # this establish a connection to Sendinblue API

        connection = self.handler.connect()
        email_campaigns_api_instance = sib_api_v3_sdk.EmailCampaignsApi(connection)

        for campaign_id in campaign_ids:
            try:
                email_campaigns_api_instance.update_email_campaign(
                    campaign_id, values_to_update
                )
                logger.info(f'Email Campaign {campaign_id} updated')
            except ApiException as e:
                logger.error(
                    f"Exception when calling EmailCampaignsApi->update_email_campaign: {e}\n"
                )
                raise RuntimeError(
                    f"Failed to execute the update command for Email Campaign {campaign_id}"
                ) from e

    def insert(self, query: 'ast.Insert') -> None:
        """
        Inserts new email campaigns into Sendinblue.

        Parameters
        ----------
        query : ast.Insert
            The SQL INSERT query to be parsed and executed.

        Raises
        ------
        ValueError
            If the necessary sender information is incomplete or incorrectly formatted.
        Exception
            For any unexpected errors during the email campaign creation.
        """
        # this defines columns that are supported and mandatory for an INSERT operation.
        supported_columns = [
            'name', 'subject', 'sender_name', 'sender_email',
            'html_content', 'scheduled_at', 'recipients_lists', 'tag'
        ]
        mandatory_columns = ['name', 'subject', 'sender_name', 'sender_email', 'html_content']

        # this Parse the INSERT query to extract data.
        insert_statement_parser = INSERTQueryParser(
            query, supported_columns=supported_columns,
            mandatory_columns=mandatory_columns, all_mandatory=True
        )
        email_campaigns_data = insert_statement_parser.parse_query()

        # this processes each campaign data extracted from the query.
        for email_campaign_data in email_campaigns_data:
            # this extracts and format sender information.
            sender_info = {}
            if 'sender_name' in email_campaign_data:
                sender_info['name'] = email_campaign_data.pop('sender_name')
            if 'sender_email' in email_campaign_data and email_campaign_data['sender_email'] is not None:
                sender_info['email'] = email_campaign_data.pop('sender_email')
            if 'sender_id' in email_campaign_data and email_campaign_data['sender_id'] is not None:
                sender_info['id'] = email_campaign_data.pop('sender_id')

            # this validates sender information.
            if not sender_info.get('name') or (not sender_info.get('email') and not sender_info.get('id')):
                raise ValueError("Sender information is incomplete or incorrectly formatted.")

            email_campaign_data['sender'] = sender_info

            # this creates each email campaign.
            self.create_email_campaign(email_campaign_data)

    def create_email_campaign(self, email_campaign_data: Dict[str, Any]) -> None:
        """
        Creates a new email campaign in Sendinblue.

        Parameters
        ----------
        email_campaign_data : Dict[str, Any]
            The data for the email campaign to be created.

        Raises
        ------
        Exception
            For any errors during the email campaign creation process.
        """
        # this establish a connection to the Sendinblue API.
        api_session = self.handler.connect()
        email_campaigns_api_instance = sib_api_v3_sdk.EmailCampaignsApi(api_session)

        # this logs the data for the email campaign being created.
        logger.info(f"Email campaign data before creating the object: {email_campaign_data}")

        try:
            # this creates the email campaign object and send it to Sendinblue.
            email_campaign = sib_api_v3_sdk.CreateEmailCampaign(**email_campaign_data)
            logger.info(f"Email campaign object after creation: {email_campaign}")

            # this executes the API call to create the campaign.
            created_campaign = email_campaigns_api_instance.create_email_campaign(email_campaign)

            # this checks and log the response from the API.
            if 'id' not in created_campaign.to_dict():
                logger.error('Email campaign creation failed')
            else:
                logger.info(f'Email Campaign {created_campaign.to_dict()["id"]} created')
        except ApiException as e:
            # this handles API exceptions and log the detailed response.
            logger.error(f"Exception when calling EmailCampaignsApi->create_email_campaign: {e}")
            if hasattr(e, 'body'):
                logger.error(f"Sendinblue API response body: {e.body}")
            raise Exception(f'Failed to create Email Campaign with data: {email_campaign_data}') from e
        except Exception as e:
            # this handles any other unexpected exceptions.
            logger.error(f"Unexpected error occurred: {e}")
            raise Exception(f'Unexpected error during Email Campaign creation: {e}') from e
