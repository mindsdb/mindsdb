from typing import List, Dict, Text, Any

import pandas as pd
from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectId as HubSpotBatchObjectIdInput,
    BatchInputSimplePublicObjectBatchInput as HubSpotBatchObjectBatchInput,
    BatchInputSimplePublicObjectInputForCreate as HubSpotBatchObjectInputCreate,

)
from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.handlers.query_utilities import (
    INSERTQueryParser,
    SELECTQueryParser,
    UPDATEQueryParser,
    DELETEQueryParser,
    SELECTQueryExecutor,
    UPDATEQueryExecutor,
    DELETEQueryExecutor,
)

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class CompaniesTable(APITable):
    """Hubspot Companies table."""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Companies data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Companies matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "companies",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        companies_df = pd.json_normalize(self.get_companies(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            companies_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        companies_df = select_statement_executor.execute_query()

        return companies_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/companies/batch/create" API endpoint.

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
            supported_columns=['name', 'city', 'phone', 'state', 'domain', 'industry'],
            mandatory_columns=['name'],
            all_mandatory=False,
        )
        company_data = insert_statement_parser.parse_query()
        self.create_companies(company_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/companies/batch/update" API endpoint.

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
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        companies_df = pd.json_normalize(self.get_companies())
        update_query_executor = UPDATEQueryExecutor(
            companies_df,
            where_conditions
        )

        companies_df = update_query_executor.execute_query()
        company_ids = companies_df['id'].tolist()
        self.update_companies(company_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/companies/batch/archive" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        companies_df = pd.json_normalize(self.get_companies())
        delete_query_executor = DELETEQueryExecutor(
            companies_df,
            where_conditions
        )

        companies_df = delete_query_executor.execute_query()
        company_ids = companies_df['id'].tolist()
        self.delete_companies(company_ids)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_companies(limit=1)).columns.tolist()

    def get_companies(self, **kwargs) -> List[Dict]:
        hubspot = self.handler.connect()
        companies = hubspot.crm.companies.get_all(**kwargs)
        companies_dict = [
            {
                "id": company.id,
                "name": company.properties.get("name", None),
                "city": company.properties.get("city", None),
                "phone": company.properties.get("phone", None),
                "state": company.properties.get("state", None),
                "domain": company.properties.get("company", None),
                "industry": company.properties.get("industry", None),
                "createdate": company.properties["createdate"],
                "lastmodifieddate": company.properties["hs_lastmodifieddate"],
            }
            for company in companies
        ]
        return companies_dict

    def create_companies(self, companies_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        companies_to_create = [HubSpotObjectInputCreate(properties=company) for company in companies_data]
        try:
            created_companies = hubspot.crm.companies.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=companies_to_create),
            )
            logger.info(f"Companies created with ID's {[created_company.id for created_company in created_companies.results]}")
        except Exception as e:
            raise Exception(f"Companies creation failed {e}")

    def update_companies(self, company_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        companies_to_update = [HubSpotObjectBatchInput(id=company_id, properties=values_to_update) for company_id in company_ids]
        try:
            updated_companies = hubspot.crm.companies.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=companies_to_update),
            )
            logger.info(f"Companies with ID {[updated_company.id for updated_company in updated_companies.results]} updated")
        except Exception as e:
            raise Exception(f"Companies update failed {e}")

    def delete_companies(self, company_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        companies_to_delete = [HubSpotObjectId(id=company_id) for company_id in company_ids]
        try:
            hubspot.crm.companies.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=companies_to_delete),
            )
            logger.info("Companies deleted")
        except Exception as e:
            raise Exception(f"Companies deletion failed {e}")


class ContactsTable(APITable):
    """Hubspot Contacts table."""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Contacts data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Contacts matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "contacts",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            contacts_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        contacts_df = select_statement_executor.execute_query()

        return contacts_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/contacts/batch/create" API endpoint.

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
            supported_columns=['email', 'firstname', 'firstname', 'phone', 'company', 'website'],
            mandatory_columns=['email'],
            all_mandatory=False,
        )
        contact_data = insert_statement_parser.parse_query()
        self.create_contacts(contact_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/contacts/batch/update" API endpoint.

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
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts())
        update_query_executor = UPDATEQueryExecutor(
            contacts_df,
            where_conditions
        )

        contacts_df = update_query_executor.execute_query()
        contact_ids = contacts_df['id'].tolist()
        self.update_contacts(contact_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/contacts/batch/archive" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        contacts_df = pd.json_normalize(self.get_contacts())
        delete_query_executor = DELETEQueryExecutor(
            contacts_df,
            where_conditions
        )

        contacts_df = delete_query_executor.execute_query()
        contact_ids = contacts_df['id'].tolist()
        self.delete_contacts(contact_ids)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_contacts(limit=1)).columns.tolist()

    def get_contacts(self, **kwargs) -> List[Dict]:
        hubspot = self.handler.connect()
        contacts = hubspot.crm.contacts.get_all(**kwargs)
        contacts_dict = [
            {
                "id": contact.id,
                "email": contact.properties["email"],
                "firstname": contact.properties.get("firstname", None),
                "lastname": contact.properties.get("lastname", None),
                "phone": contact.properties.get("phone", None),
                "company": contact.properties.get("company", None),
                "website": contact.properties.get("website", None),
                "createdate": contact.properties["createdate"],
                "lastmodifieddate": contact.properties["lastmodifieddate"],
            }
            for contact in contacts
        ]
        return contacts_dict

    def create_contacts(self, contacts_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        contacts_to_create = [HubSpotObjectInputCreate(properties=contact) for contact in contacts_data]
        try:
            created_contacts = hubspot.crm.contacts.batch_api.create(
                HubSpotBatchObjectInputCreate(inputs=contacts_to_create)
            )
            logger.info(f"Contacts created with ID {[created_contact.id for created_contact in created_contacts.results]}")
        except Exception as e:
            raise Exception(f"Contacts creation failed {e}")

    def update_contacts(self, contact_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        contacts_to_update = [HubSpotObjectBatchInput(id=contact_id, properties=values_to_update) for contact_id in contact_ids]
        try:
            updated_contacts = hubspot.crm.contacts.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=contacts_to_update),
            )
            logger.info(f"Contacts with ID {[updated_contact.id for updated_contact in updated_contacts.results]} updated")
        except Exception as e:
            raise Exception(f"Contacts update failed {e}")

    def delete_contacts(self, contact_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        contacts_to_delete = [HubSpotObjectId(id=contact_id) for contact_id in contact_ids]
        try:
            hubspot.crm.contacts.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=contacts_to_delete),
            )
            logger.info("Contacts deleted")
        except Exception as e:
            raise Exception(f"Contacts deletion failed {e}")


class DealsTable(APITable):
    """Hubspot Deals table."""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Hubspot Deals data

        Parameters
        ----------
        query : ast.Select
            Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Hubspot Deals matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition

        """

        select_statement_parser = SELECTQueryParser(
            query,
            "deals",
            self.get_columns()
        )
        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        deals_df = pd.json_normalize(self.get_deals(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            deals_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )
        deals_df = select_statement_executor.execute_query()

        return deals_df

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts data into HubSpot "POST /crm/v3/objects/deals/batch/create" API endpoint.

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
            supported_columns=['amount', 'dealname', 'pipeline', 'closedate', 'dealstage', 'hubspot_owner_id'],
            mandatory_columns=['dealname'],
            all_mandatory=False,
        )
        deals_data = insert_statement_parser.parse_query()
        self.create_deals(deals_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates data from HubSpot "PATCH /crm/v3/objects/deals/batch/update" API endpoint.

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
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        deals_df = pd.json_normalize(self.get_deals())
        update_query_executor = UPDATEQueryExecutor(
            deals_df,
            where_conditions
        )

        deals_df = update_query_executor.execute_query()
        deal_ids = deals_df['id'].tolist()
        self.update_deals(deal_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from HubSpot "DELETE /crm/v3/objects/deals/batch/archive" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        deals_df = pd.json_normalize(self.get_deals())
        delete_query_executor = DELETEQueryExecutor(
            deals_df,
            where_conditions
        )

        deals_df = delete_query_executor.execute_query()
        deal_ids = deals_df['id'].tolist()
        self.delete_deals(deal_ids)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_deals(limit=1)).columns.tolist()

    def get_deals(self, **kwargs) -> List[Dict]:
        hubspot = self.handler.connect()
        deals = hubspot.crm.deals.get_all(**kwargs)
        deals_dict = [
            {
                "id": deal.id,
                "dealname": deal.properties["dealname"],
                "amount": deal.properties.get("amount", None),
                "pipeline": deal.properties.get("pipeline", None),
                "closedate": deal.properties.get("closedate", None),
                "dealstage": deal.properties.get("dealstage", None),
                "hubspot_owner_id": deal.properties.get("hubspot_owner_id", None),
                "createdate": deal.properties["createdate"],
                "hs_lastmodifieddate": deal.properties["hs_lastmodifieddate"],
            }
            for deal in deals
        ]
        return deals_dict

    def create_deals(self, deals_data: List[Dict[Text, Any]]) -> None:
        hubspot = self.handler.connect()
        deals_to_create = [HubSpotObjectInputCreate(properties=deal) for deal in deals_data]
        try:
            created_deals = hubspot.crm.deals.batch_api.create(
                HubSpotBatchObjectBatchInput(inputs=deals_to_create),
            )
            logger.info(f"Deals created with ID's {[created_deal.id for created_deal in created_deals.results]}")
        except Exception as e:
            raise Exception(f"Deals creation failed {e}")

    def update_deals(self, deal_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        deals_to_update = [HubSpotObjectBatchInput(id=deal_id, properties=values_to_update) for deal_id in deal_ids]
        try:
            updated_deals = hubspot.crm.deals.batch_api.update(
                HubSpotBatchObjectBatchInput(inputs=deals_to_update),
            )
            logger.info(f"Deals with ID {[updated_deal.id for updated_deal in updated_deals.results]} updated")
        except Exception as e:
            raise Exception(f"Deals update failed {e}")

    def delete_deals(self, deal_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        deals_to_delete = [HubSpotObjectId(id=deal_id) for deal_id in deal_ids]
        try:
            hubspot.crm.deals.batch_api.archive(
                HubSpotBatchObjectIdInput(inputs=deals_to_delete),
            )
            logger.info("Deals deleted")
        except Exception as e:
            raise Exception(f"Deals deletion failed {e}")
