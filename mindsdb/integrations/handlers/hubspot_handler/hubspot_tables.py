from typing import List, Dict, Text, Any, Optional, Tuple, Set

import pandas as pd
from hubspot import HubSpot
from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectBatchInputForCreate,
    BatchInputSimplePublicObjectBatchInput,
    BatchInputSimplePublicObjectId,
)
from mindsdb.integrations.utilities.handlers.query_utilities import UPDATEQueryExecutor, DELETEQueryExecutor
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn
from mindsdb.utilities import log

logger = log.getLogger(__name__)

HUBSPOT_TABLE_COLUMN_DEFINITIONS: Dict[str, List[Tuple[str, str, str]]] = {
    "companies": [
        ("name", "VARCHAR", "Company name"),
        ("domain", "VARCHAR", "Company domain"),
        ("industry", "VARCHAR", "Industry"),
        ("city", "VARCHAR", "City"),
        ("state", "VARCHAR", "State"),
        ("phone", "VARCHAR", "Phone number"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    "contacts": [
        ("email", "VARCHAR", "Email address"),
        ("firstname", "VARCHAR", "First name"),
        ("lastname", "VARCHAR", "Last name"),
        ("phone", "VARCHAR", "Phone number"),
        ("company", "VARCHAR", "Associated company"),
        ("city", "VARCHAR", "City"),
        ("website", "VARCHAR", "Website URL"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
    "deals": [
        ("dealname", "VARCHAR", "Deal name"),
        ("amount", "DECIMAL", "Deal amount"),
        ("dealstage", "VARCHAR", "Deal stage"),
        ("pipeline", "VARCHAR", "Sales pipeline"),
        ("closedate", "DATE", "Expected close date"),
        ("hubspot_owner_id", "VARCHAR", "Owner ID"),
        ("createdate", "TIMESTAMP", "Creation date"),
        ("lastmodifieddate", "TIMESTAMP", "Last modification date"),
    ],
}


def _normalize_filter_conditions(conditions: Optional[List[FilterCondition]]) -> List[List[Any]]:
    """
    Convert FilterCondition instances into the condition format expected by query executors.
    """
    normalized: List[List[Any]] = []
    if not conditions:
        return normalized

    for condition in conditions:
        if isinstance(condition, FilterCondition):
            normalized.append([condition.op.value, condition.column, condition.value])
        elif isinstance(condition, (list, tuple)) and len(condition) >= 3:
            normalized.append([condition[0], condition[1], condition[2]])
    return normalized


OPERATOR_MAP = {
    "=": "EQ",
    "!=": "NEQ",
    "<": "LT",
    "<=": "LTE",
    ">": "GT",
    ">=": "GTE",
    "in": "IN",
    "not in": "NOT_IN",
}


def _build_hubspot_search_filters(
    conditions: List[List[Any]],
    searchable_columns: Set[str],
) -> Optional[List[Dict]]:
    """
    Convert normalized conditions to HubSpot Search API filter format.

    Returns a list of filter dicts if all conditions are supported, otherwise None.
    """
    if not conditions:
        return None

    filters: List[Dict[str, Any]] = []

    for condition in conditions:
        if not isinstance(condition, (list, tuple)) or len(condition) < 3:
            return None

        operator, column, value = condition[0], condition[1], condition[2]
        operator_key = str(operator).lower()
        if operator_key not in OPERATOR_MAP:
            return None

        if column not in searchable_columns:
            return None

        property_name = "hs_object_id" if column == "id" else column
        if column == "lastmodifieddate":
            property_name = "hs_lastmodifieddate"

        hubspot_operator = OPERATOR_MAP[operator_key]
        if hubspot_operator in {"IN", "NOT_IN"}:
            values = value if isinstance(value, (list, tuple, set)) else [value]
            filters.append(
                {
                    "propertyName": property_name,
                    "operator": hubspot_operator,
                    "values": [str(val) for val in values],
                }
            )
        else:
            filters.append(
                {
                    "propertyName": property_name,
                    "operator": hubspot_operator,
                    "value": str(value),
                }
            )

    if not filters:
        return None

    return filters


def _execute_hubspot_search(
    search_api,
    filters: List[Dict],
    properties: List[str],
    limit: Optional[int],
    to_dict_fn: callable,
) -> List[Dict[str, Any]]:
    """
    Execute paginated HubSpot search with filters.
    """
    collected: List[Dict[str, Any]] = []
    remaining = limit if limit is not None else float("inf")
    after = None

    while remaining > 0:
        page_limit = min(int(remaining) if remaining != float("inf") else 200,200)
        search_request = {
            "filterGroups": [{"filters": filters}],
            "properties": properties,
            "limit": page_limit,
        }

        if after is not None:
            search_request["after"] = after

        response = search_api.do_search(public_object_search_request=search_request)

        results = getattr(response, "results", []) or []
        for result in results:
            collected.append(to_dict_fn(result))
            if limit is not None and len(collected) >= limit:
                return collected

        paging = getattr(response, "paging", None)
        next_page = getattr(paging, "next", None) if paging else None
        after = getattr(next_page, "after", None) if next_page else None

        if after is None:
            break

        if remaining != float("inf"):
            remaining = limit - len(collected)

    return collected


class CompaniesTable(APIResource):
    """Hubspot Companies table."""

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        """Return static metadata for the companies table."""
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("companies")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot companies row count: {e}")

        return {
            "TABLE_NAME": "companies",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": self.handler._get_table_description("companies"),
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Return default column metadata for companies."""
        return self.handler._get_default_meta_columns("companies")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        companies_df = pd.json_normalize(self.get_companies(limit=limit, where_conditions=conditions))
        if companies_df.empty:
            companies_df = pd.DataFrame(columns=self._get_default_company_columns())
        return companies_df

    def add(self, company_data: List[dict]):
        self.create_companies(company_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        companies_df = pd.json_normalize(self.get_companies(limit=200, where_conditions=normalized_conditions))

        if companies_df.empty:
            raise ValueError(
                "No companies retrieved from HubSpot to evaluate update conditions. Verify your connection and permissions."
            )

        update_query_executor = UPDATEQueryExecutor(companies_df, normalized_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(
                f"No companies found matching WHERE conditions: {conditions}. Please verify the conditions are correct."
            )

        company_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(company_ids)} compan(ies) matching WHERE conditions")
        self.update_companies(company_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        normalized_conditions = _normalize_filter_conditions(conditions)
        companies_df = pd.json_normalize(self.get_companies(limit=200, where_conditions=normalized_conditions))

        if companies_df.empty:
            raise ValueError(
                "No companies retrieved from HubSpot to evaluate delete conditions. Verify your connection and permissions."
            )

        delete_query_executor = DELETEQueryExecutor(companies_df, normalized_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(
                f"No companies found matching WHERE conditions: {conditions}. Please verify the conditions are correct."
            )

        company_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(company_ids)} compan(ies) matching WHERE conditions")
        self.delete_companies(company_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_company_columns()

    @staticmethod
    def _get_default_company_columns() -> List[str]:
        return [
            "id",
            "name",
            "city",
            "phone",
            "state",
            "domain",
            "industry",
            "createdate",
            "lastmodifieddate",
        ]

    def get_companies(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()

        requested_properties = kwargs.get("properties", [])
        default_properties = [
            "name",
            "domain",
            "industry",
            "city",
            "state",
            "phone",
            "createdate",
            "hs_lastmodifieddate",
        ]

        properties = list({*default_properties, *requested_properties})

        api_kwargs = {**kwargs, "properties": properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if normalized_conditions:
            filters = _build_hubspot_search_filters(
                normalized_conditions, {"name", "domain", "industry", "city", "state", "id"}
            )
            if filters:
                search_results = self._search_companies_by_conditions(hubspot, filters, properties, limit)
                logger.info(f"Retrieved {len(search_results)} companies from HubSpot via search API")
                return search_results

        companies = hubspot.crm.companies.get_all(**api_kwargs)
        companies_dict = []

        for company in companies:
            try:
                companies_dict.append(self._company_to_dict(company))
            except Exception as e:
                logger.warning(f"Error processing company {getattr(company, 'id', 'unknown')}: {str(e)}")
                continue

        logger.info(f"Retrieved {len(companies_dict)} companies from HubSpot")
        return companies_dict

    def _search_companies_by_conditions(
        self,
        hubspot: HubSpot,
        filters: List[Dict[str, Any]],
        properties: List[str],
        limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.companies.search_api,
            filters,
            properties,
            limit,
            self._company_to_dict,
        )

    def _company_to_dict(self, company: Any) -> Dict[str, Any]:
        properties = getattr(company, "properties", {}) or {}
        return {
            "id": company.id,
            "name": properties.get("name"),
            "city": properties.get("city"),
            "phone": properties.get("phone"),
            "state": properties.get("state"),
            "domain": properties.get("domain"),
            "industry": properties.get("industry"),
            "createdate": properties.get("createdate"),
            "lastmodifieddate": properties.get("hs_lastmodifieddate"),
        }

    def create_companies(self, companies_data: List[Dict[Text, Any]]) -> None:
        if not companies_data:
            raise ValueError("No company data provided for creation")

        logger.info(f"Attempting to create {len(companies_data)} compan(ies): {companies_data}")

        hubspot = self.handler.connect()
        companies_to_create = [HubSpotObjectInputCreate(properties=company) for company in companies_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=companies_to_create)

        try:
            created_companies = hubspot.crm.companies.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )

            if not created_companies or not hasattr(created_companies, "results") or not created_companies.results:
                raise Exception("Company creation returned no results")

            created_ids = [created_company.id for created_company in created_companies.results]
            logger.info(f"Successfully created {len(created_ids)} compan(ies) with IDs: {created_ids}")

        except Exception as e:
            logger.error(f"Companies creation failed: {str(e)}")
            raise Exception(f"Companies creation failed {e}")

    def update_companies(self, company_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        companies_to_update = [
            HubSpotObjectBatchInput(id=company_id, properties=values_to_update) for company_id in company_ids
        ]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=companies_to_update)
        try:
            updated_companies = hubspot.crm.companies.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(
                f"Companies with ID {[updated_company.id for updated_company in updated_companies.results]} updated"
            )
        except Exception as e:
            raise Exception(f"Companies update failed {e}")

    def delete_companies(self, company_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        companies_to_delete = [HubSpotObjectId(id=company_id) for company_id in company_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=companies_to_delete)
        try:
            hubspot.crm.companies.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Companies deleted")
        except Exception as e:
            raise Exception(f"Companies deletion failed {e}")


class ContactsTable(APIResource):
    """Hubspot Contacts table."""

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("contacts")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot contacts row count: {e}")

        return {
            "TABLE_NAME": "contacts",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": self.handler._get_table_description("contacts"),
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("contacts")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        requested_properties = targets or []
        contacts_df = pd.json_normalize(
            self.get_contacts(limit=limit, where_conditions=conditions, properties=requested_properties)
        )
        if contacts_df.empty:
            contacts_df = pd.DataFrame(columns=self._get_default_contact_columns())
        else:
            contacts_df["id"] = pd.to_numeric(contacts_df["id"], errors="coerce")
        return contacts_df

    def add(self, contact_data: List[dict]):
        self.create_contacts(contact_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        contacts_df = pd.json_normalize(self.get_contacts(limit=200, where_conditions=where_conditions))

        if contacts_df.empty:
            raise ValueError(
                "No contacts retrieved from HubSpot to evaluate update conditions. Verify your connection and permissions."
            )

        update_query_executor = UPDATEQueryExecutor(contacts_df, where_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(
                f"No contacts found matching WHERE conditions: {conditions}. Please verify the conditions are correct."
            )

        contact_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(contact_ids)} contact(s) matching WHERE conditions")
        self.update_contacts(contact_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        contacts_df = pd.json_normalize(self.get_contacts(limit=200, where_conditions=where_conditions))

        if contacts_df.empty:
            raise ValueError(
                "No contacts retrieved from HubSpot to evaluate delete conditions. Verify your connection and permissions."
            )

        delete_query_executor = DELETEQueryExecutor(contacts_df, where_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(
                f"No contacts found matching WHERE conditions: {conditions}. Please verify the conditions are correct."
            )

        contact_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(contact_ids)} contact(s) matching WHERE conditions")
        self.delete_contacts(contact_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_contact_columns()

    @staticmethod
    def _get_default_contact_columns() -> List[str]:
        return [
            "id",
            "email",
            "firstname",
            "lastname",
            "phone",
            "company",
            "city",
            "website",
            "createdate",
            "lastmodifieddate",
        ]

    def get_contacts(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()
        requested_properties = kwargs.get("properties", [])
        default_properties = [
            "email",
            "firstname",
            "lastname",
            "phone",
            "company",
            "city",
            "website",
            "createdate",
            "lastmodifieddate",
        ]

        properties = list({*default_properties, *requested_properties})

        api_kwargs = {**kwargs, "properties": properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if normalized_conditions:
            filters = _build_hubspot_search_filters(
                normalized_conditions, {"email", "id", "firstname", "lastname", "phone", "company", "city"}
            )
            if filters:
                search_results = self._search_contacts_by_conditions(hubspot, filters, properties, limit)
                logger.info(f"Retrieved {len(search_results)} contacts from HubSpot via search API")
                return search_results

        contacts = hubspot.crm.contacts.get_all(**api_kwargs)
        contacts_dict = []

        try:
            for contact in contacts:
                contacts_dict.append(self._contact_to_dict(contact))
                if limit is not None and len(contacts_dict) >= limit:
                    break
        except Exception as e:
            logger.error(f"Failed to iterate HubSpot contacts: {str(e)}")
            raise

        logger.info(f"Retrieved {len(contacts_dict)} contacts from HubSpot")
        return contacts_dict

    def _search_contacts_by_conditions(
        self,
        hubspot: HubSpot,
        filters: List[Dict[str, Any]],
        properties: List[str],
        limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.contacts.search_api,
            filters,
            properties,
            limit,
            self._contact_to_dict,
        )

    def _contact_to_dict(self, contact: Any) -> Dict[str, Any]:
        try:
            properties = getattr(contact, "properties", {}) or {}
            return {
                "id": contact.id,
                "email": properties.get("email"),
                "firstname": properties.get("firstname"),
                "lastname": properties.get("lastname"),
                "phone": properties.get("phone"),
                "company": properties.get("company"),
                "city": properties.get("city"),
                "website": properties.get("website"),
                "createdate": properties.get("createdate"),
                "lastmodifieddate": properties.get("lastmodifieddate"),
            }
        except Exception as e:
            logger.warning(f"Error processing contact {getattr(contact, 'id', 'unknown')}: {str(e)}")
            return {
                "id": getattr(contact, "id", None),
                "email": None,
                "firstname": None,
                "lastname": None,
                "phone": None,
                "company": None,
                "city": None,
                "website": None,
                "createdate": None,
                "lastmodifieddate": None,
            }

    def create_contacts(self, contacts_data: List[Dict[Text, Any]]) -> None:
        if not contacts_data:
            raise ValueError("No contact data provided for creation")

        logger.info(f"Attempting to create {len(contacts_data)} contact(s): {contacts_data}")

        hubspot = self.handler.connect()
        contacts_to_create = [HubSpotObjectInputCreate(properties=contact) for contact in contacts_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=contacts_to_create)

        try:
            created_contacts = hubspot.crm.contacts.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )

            if not created_contacts or not hasattr(created_contacts, "results") or not created_contacts.results:
                raise Exception("Contact creation returned no results")

            created_ids = [created_contact.id for created_contact in created_contacts.results]
            logger.info(f"Successfully created {len(created_ids)} contact(s) with IDs: {created_ids}")

        except Exception as e:
            logger.error(f"Contacts creation failed: {str(e)}")
            raise Exception(f"Contacts creation failed {e}")

    def update_contacts(self, contact_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        contacts_to_update = [
            HubSpotObjectBatchInput(id=contact_id, properties=values_to_update) for contact_id in contact_ids
        ]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=contacts_to_update)
        try:
            updated_contacts = hubspot.crm.contacts.batch_api.update(
                batch_input_simple_public_object_batch_input=batch_input
            )
            logger.info(
                f"Contacts with ID {[updated_contact.id for updated_contact in updated_contacts.results]} updated"
            )
        except Exception as e:
            raise Exception(f"Contacts update failed {e}")

    def delete_contacts(self, contact_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        contacts_to_delete = [HubSpotObjectId(id=contact_id) for contact_id in contact_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=contacts_to_delete)
        try:
            hubspot.crm.contacts.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Contacts deleted")
        except Exception as e:
            raise Exception(f"Contacts deletion failed {e}")


class DealsTable(APIResource):
    """Hubspot Deals table."""

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        row_count = None
        try:
            self.handler.connect()
            row_count = self.handler._estimate_table_rows("deals")
        except Exception as e:
            logger.warning(f"Could not estimate HubSpot deals row count: {e}")

        return {
            "TABLE_NAME": "deals",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": self.handler._get_table_description("deals"),
            "ROW_COUNT": row_count,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return self.handler._get_default_meta_columns("deals")

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        deals_df = pd.json_normalize(self.get_deals(limit=limit, where_conditions=conditions))
        if deals_df.empty:
            deals_df = pd.DataFrame(columns=self._get_default_deal_columns())
        else:
            deals_df = self._cast_deal_columns(deals_df)
        return deals_df

    def add(self, deal_data: List[dict]):
        self.create_deals(deal_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        deals_df = pd.json_normalize(self.get_deals(limit=200, where_conditions=where_conditions))

        if deals_df.empty:
            raise ValueError(
                "No deals retrieved from HubSpot to evaluate update conditions. Verify your connection and permissions."
            )

        update_query_executor = UPDATEQueryExecutor(deals_df, where_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(
                f"No deals found matching WHERE conditions: {conditions}. Please verify the conditions are correct."
            )

        deal_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(deal_ids)} deal(s) matching WHERE conditions")
        self.update_deals(deal_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        deals_df = pd.json_normalize(self.get_deals(limit=200, where_conditions=where_conditions))

        if deals_df.empty:
            raise ValueError(
                "No deals retrieved from HubSpot to evaluate delete conditions. Verify your connection and permissions."
            )

        delete_query_executor = DELETEQueryExecutor(deals_df, where_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(
                f"No deals found matching WHERE conditions: {conditions}. Please verify the conditions are correct."
            )

        deal_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(deal_ids)} deal(s) matching WHERE conditions")
        self.delete_deals(deal_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_deal_columns()

    @staticmethod
    def _get_default_deal_columns() -> List[str]:
        return [
            "id",
            "dealname",
            "amount",
            "pipeline",
            "closedate",
            "dealstage",
            "hubspot_owner_id",
            "createdate",
            "hs_lastmodifieddate",
        ]

    @staticmethod
    def _cast_deal_columns(deals_df: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = ["amount"]
        datetime_columns = ["closedate", "createdate", "hs_lastmodifieddate"]

        for column in numeric_columns:
            if column in deals_df.columns:
                deals_df[column] = pd.to_numeric(deals_df[column], errors="coerce")

        for column in datetime_columns:
            if column in deals_df.columns:
                deals_df[column] = pd.to_datetime(deals_df[column], errors="coerce")

        return deals_df

    def get_deals(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()
        requested_properties = kwargs.get("properties", [])
        default_properties = [
            "dealname",
            "amount",
            "pipeline",
            "closedate",
            "dealstage",
            "hubspot_owner_id",
            "createdate",
            "hs_lastmodifieddate",
        ]

        properties = list({*default_properties, *requested_properties})
        api_kwargs = {**kwargs, "properties": properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)

        if normalized_conditions:
            filters = _build_hubspot_search_filters(
                normalized_conditions, {"dealname", "amount", "dealstage", "pipeline", "closedate", "id"}
            )
            if filters:
                search_results = self._search_deals_by_conditions(hubspot, filters, properties, limit)
                logger.info(f"Retrieved {len(search_results)} deals from HubSpot via search API")
                return search_results

        deals = hubspot.crm.deals.get_all(**api_kwargs)
        deals_dict = []

        for deal in deals:
            try:
                deals_dict.append(self._deal_to_dict(deal))
            except Exception as e:
                logger.error(f"Error processing deal {getattr(deal, 'id', 'unknown')}: {str(e)}")
                raise ValueError(
                    f"Failed to process deal {getattr(deal, 'id', 'unknown')}. "
                    f"Please verify the HubSpot record and try again."
                ) from e

        logger.info(f"Retrieved {len(deals_dict)} deals from HubSpot")
        return deals_dict

    def _search_deals_by_conditions(
        self,
        hubspot: HubSpot,
        filters: List[Dict[str, Any]],
        properties: List[str],
        limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        return _execute_hubspot_search(
            hubspot.crm.deals.search_api,
            filters,
            properties,
            limit,
            self._deal_to_dict,
        )

    def _deal_to_dict(self, deal: Any) -> Dict[str, Any]:
        properties = getattr(deal, "properties", {}) or {}
        return {
            "id": deal.id,
            "dealname": properties.get("dealname", None),
            "amount": properties.get("amount", None),
            "pipeline": properties.get("pipeline", None),
            "closedate": properties.get("closedate", None),
            "dealstage": properties.get("dealstage", None),
            "hubspot_owner_id": properties.get("hubspot_owner_id", None),
            "createdate": properties.get("createdate", None),
            "hs_lastmodifieddate": properties.get("hs_lastmodifieddate", None),
        }

    def create_deals(self, deals_data: List[Dict[Text, Any]]) -> None:
        if not deals_data:
            raise ValueError("No deal data provided for creation")

        logger.info(f"Attempting to create {len(deals_data)} deal(s): {deals_data}")

        hubspot = self.handler.connect()
        deals_to_create = [HubSpotObjectInputCreate(properties=deal) for deal in deals_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=deals_to_create)

        try:
            created_deals = hubspot.crm.deals.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )

            if not created_deals or not hasattr(created_deals, "results") or not created_deals.results:
                raise Exception("Deal creation returned no results")

            created_ids = [created_deal.id for created_deal in created_deals.results]
            logger.info(f"Successfully created {len(created_ids)} deal(s) with IDs: {created_ids}")

        except Exception as e:
            logger.error(f"Deals creation failed: {str(e)}")
            raise Exception(f"Deals creation failed {e}")

    def update_deals(self, deal_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        deals_to_update = [HubSpotObjectBatchInput(id=deal_id, properties=values_to_update) for deal_id in deal_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=deals_to_update)
        try:
            updated_deals = hubspot.crm.deals.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Deals with ID {[updated_deal.id for updated_deal in updated_deals.results]} updated")
        except Exception as e:
            raise Exception(f"Deals update failed {e}")

    def delete_deals(self, deal_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        deals_to_delete = [HubSpotObjectId(id=deal_id) for deal_id in deal_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=deals_to_delete)
        try:
            hubspot.crm.deals.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Deals deleted")
        except Exception as e:
            raise Exception(f"Deals deletion failed {e}")
