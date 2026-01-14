"""
HubSpot Association Tables for MindsDB.

This module provides association tables that expose the many-to-many relationships
between HubSpot CRM objects (companies, contacts, deals, tickets, etc.).

Reference: https://developers.hubspot.com/docs/api/crm/associations
"""

from typing import List, Dict, Text, Any, Optional, Set
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn
from mindsdb.utilities import log

logger = log.getLogger(__name__)


# HubSpot association type IDs (v4 API)
# Reference: https://developers.hubspot.com/docs/api/crm/associations#association-type-id-values
ASSOCIATION_TYPES = {
    # Company associations
    "company_to_contact": 280,
    "company_to_deal": 342,
    "company_to_ticket": 340,
    "company_to_call": 182,
    "company_to_email": 186,
    "company_to_meeting": 190,
    "company_to_note": 190,
    "company_to_task": 192,
    # Contact associations
    "contact_to_company": 279,
    "contact_to_deal": 4,
    "contact_to_ticket": 16,
    "contact_to_call": 194,
    "contact_to_email": 198,
    "contact_to_meeting": 200,
    "contact_to_note": 202,
    "contact_to_task": 204,
    # Deal associations
    "deal_to_company": 341,
    "deal_to_contact": 3,
    "deal_to_ticket": 28,
    "deal_to_call": 206,
    "deal_to_email": 210,
    "deal_to_meeting": 212,
    "deal_to_note": 214,
    "deal_to_task": 216,
    # Ticket associations
    "ticket_to_company": 339,
    "ticket_to_contact": 15,
    "ticket_to_deal": 27,
}


class HubSpotAssociationTable(APIResource):
    """
    Base class for HubSpot association tables.
    
    Association tables expose the many-to-many relationships between HubSpot objects.
    They are read-only views that fetch association data from the HubSpot API.
    """
    
    # Subclasses must define these
    FROM_OBJECT_TYPE: str = ""
    TO_OBJECT_TYPE: str = ""
    FROM_ID_COLUMN: str = ""
    TO_ID_COLUMN: str = ""
    
    def get_columns(self) -> List[Text]:
        """Return column names for the association table."""
        return [self.FROM_ID_COLUMN, self.TO_ID_COLUMN, "association_type", "association_label"]
    
    def select(self, query) -> pd.DataFrame:
        """Execute SELECT query on association table."""
        # Extract limit from query
        result_limit = query.limit.value if query.limit else None
        
        # For association tables, we fetch all associations and filter in memory
        # since HubSpot doesn't support filtering on association endpoints
        return self.list(limit=result_limit)
    
    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch associations between objects."""
        associations = self._fetch_associations(limit=limit)
        
        if not associations:
            return pd.DataFrame(columns=self.get_columns())
        
        df = pd.DataFrame(associations)
        
        # Apply any conditions for filtering (basic support)
        if conditions:
            df = self._apply_conditions(df, conditions)
        
        return df
    
    def _fetch_associations(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch associations by getting source objects with their associations.
        
        This approach fetches the source objects with associations included,
        which is more efficient than making separate association API calls.
        """
        hubspot = self.handler.connect()
        results = []
        
        try:
            # Determine which API to use based on object type
            if self.FROM_OBJECT_TYPE in ["companies", "contacts", "deals", "tickets"]:
                # Core CRM objects use dedicated endpoints
                source_objects = getattr(hubspot.crm, self.FROM_OBJECT_TYPE).get_all(
                    associations=[self.TO_OBJECT_TYPE],
                    limit=limit or 500,
                )
            else:
                # Engagement objects use generic objects API
                source_objects = self.handler._get_objects_all(
                    self.FROM_OBJECT_TYPE,
                    associations=[self.TO_OBJECT_TYPE],
                    limit=limit or 500,
                )
            
            for obj in source_objects:
                from_id = obj.id
                associations = getattr(obj, "associations", None)
                
                if not associations:
                    continue
                
                # Handle different association response structures
                to_objects = None
                if isinstance(associations, dict):
                    to_objects = associations.get(self.TO_OBJECT_TYPE)
                else:
                    to_objects = getattr(associations, self.TO_OBJECT_TYPE, None)

                if to_objects is None:
                    continue

                if isinstance(to_objects, dict):
                    to_objects = to_objects.get("results") or to_objects.get("items") or []
                elif hasattr(to_objects, "results"):
                    to_objects = to_objects.results or []
                elif hasattr(to_objects, "items"):
                    to_objects = to_objects.items or []
                
                if not to_objects:
                    continue
                
                for assoc in to_objects:
                    if isinstance(assoc, dict):
                        to_id = assoc.get("id") or assoc.get("toObjectId") or assoc.get("to_object_id")
                    else:
                        to_id = (
                            getattr(assoc, "id", None)
                            or getattr(assoc, "toObjectId", None)
                            or getattr(assoc, "to_object_id", None)
                        )
                    if not to_id:
                        continue
                    
                    # Extract association type info
                    assoc_type = None
                    assoc_label = None
                    
                    if hasattr(assoc, "type"):
                        assoc_type = assoc.type
                    elif isinstance(assoc, dict):
                        assoc_type = assoc.get("type")
                    
                    # Try to get label from association types
                    if hasattr(assoc, "associationTypes") and assoc.associationTypes:
                        for at in assoc.associationTypes:
                            assoc_label = getattr(at, "label", assoc_label)
                            assoc_type = getattr(at, "typeId", assoc_type)
                            break
                    elif isinstance(assoc, dict):
                        assoc_types = assoc.get("associationTypes") or assoc.get("association_types") or []
                        if assoc_types:
                            assoc_entry = assoc_types[0]
                            if isinstance(assoc_entry, dict):
                                assoc_label = assoc_label or assoc_entry.get("label")
                                assoc_type = assoc_type or assoc_entry.get("typeId") or assoc_entry.get("type")
                    
                    results.append({
                        self.FROM_ID_COLUMN: str(from_id),
                        self.TO_ID_COLUMN: str(to_id),
                        "association_type": assoc_type,
                        "association_label": assoc_label,
                    })
                
                if limit and len(results) >= limit:
                    break
            
            logger.info(
                f"Retrieved {len(results)} {self.FROM_OBJECT_TYPE}->{self.TO_OBJECT_TYPE} associations"
            )
            return results
            
        except Exception as e:
            logger.error(f"Failed to fetch associations: {str(e)}")
            raise
    
    def _apply_conditions(self, df: pd.DataFrame, conditions: List[FilterCondition]) -> pd.DataFrame:
        """Apply filter conditions to the DataFrame."""
        if df.empty:
            return df
        
        for condition in conditions:
            column = condition.column if hasattr(condition, "column") else condition[1]
            op = str(condition.op if hasattr(condition, "op") else condition[0]).lower()
            value = condition.value if hasattr(condition, "value") else condition[2]
            
            if column not in df.columns:
                continue
            
            if op in ("=", "==", "eq"):
                df = df[df[column] == str(value)]
            elif op in ("!=", "<>", "neq"):
                df = df[df[column] != str(value)]
            elif op == "in":
                values = list(value) if isinstance(value, (list, tuple, set)) else [value]
                df = df[df[column].isin([str(v) for v in values])]
        
        return df
    
    def add(self, data: List[dict]) -> None:
        """Create associations - not yet implemented."""
        raise NotImplementedError(
            "Creating associations via INSERT is not yet supported. "
            "Use the HubSpot API directly to create associations."
        )
    
    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        """Update associations - not applicable."""
        raise NotImplementedError("Associations cannot be updated. Delete and recreate instead.")
    
    def remove(self, conditions: List[FilterCondition]) -> None:
        """Delete associations - not yet implemented."""
        raise NotImplementedError(
            "Deleting associations via DELETE is not yet supported. "
            "Use the HubSpot API directly to remove associations."
        )


# =============================================================================
# Company Association Tables
# =============================================================================

class CompanyContactsTable(HubSpotAssociationTable):
    """Association table for company-contact relationships."""
    
    FROM_OBJECT_TYPE = "companies"
    TO_OBJECT_TYPE = "contacts"
    FROM_ID_COLUMN = "company_id"
    TO_ID_COLUMN = "contact_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "company_contacts",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking companies to their contacts",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "company_contacts", "COLUMN_NAME": "company_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Company ID"},
            {"TABLE_NAME": "company_contacts", "COLUMN_NAME": "contact_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Contact ID"},
            {"TABLE_NAME": "company_contacts", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "company_contacts", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


class CompanyDealsTable(HubSpotAssociationTable):
    """Association table for company-deal relationships."""
    
    FROM_OBJECT_TYPE = "companies"
    TO_OBJECT_TYPE = "deals"
    FROM_ID_COLUMN = "company_id"
    TO_ID_COLUMN = "deal_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "company_deals",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking companies to their deals",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "company_deals", "COLUMN_NAME": "company_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Company ID"},
            {"TABLE_NAME": "company_deals", "COLUMN_NAME": "deal_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Deal ID"},
            {"TABLE_NAME": "company_deals", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "company_deals", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


class CompanyTicketsTable(HubSpotAssociationTable):
    """Association table for company-ticket relationships."""
    
    FROM_OBJECT_TYPE = "companies"
    TO_OBJECT_TYPE = "tickets"
    FROM_ID_COLUMN = "company_id"
    TO_ID_COLUMN = "ticket_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "company_tickets",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking companies to their tickets",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "company_tickets", "COLUMN_NAME": "company_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Company ID"},
            {"TABLE_NAME": "company_tickets", "COLUMN_NAME": "ticket_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Ticket ID"},
            {"TABLE_NAME": "company_tickets", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "company_tickets", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


# =============================================================================
# Contact Association Tables
# =============================================================================

class ContactCompaniesTable(HubSpotAssociationTable):
    """Association table for contact-company relationships."""
    
    FROM_OBJECT_TYPE = "contacts"
    TO_OBJECT_TYPE = "companies"
    FROM_ID_COLUMN = "contact_id"
    TO_ID_COLUMN = "company_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "contact_companies",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking contacts to their companies",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "contact_companies", "COLUMN_NAME": "contact_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Contact ID"},
            {"TABLE_NAME": "contact_companies", "COLUMN_NAME": "company_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Company ID"},
            {"TABLE_NAME": "contact_companies", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "contact_companies", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


class ContactDealsTable(HubSpotAssociationTable):
    """Association table for contact-deal relationships."""
    
    FROM_OBJECT_TYPE = "contacts"
    TO_OBJECT_TYPE = "deals"
    FROM_ID_COLUMN = "contact_id"
    TO_ID_COLUMN = "deal_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "contact_deals",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking contacts to their deals",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "contact_deals", "COLUMN_NAME": "contact_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Contact ID"},
            {"TABLE_NAME": "contact_deals", "COLUMN_NAME": "deal_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Deal ID"},
            {"TABLE_NAME": "contact_deals", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "contact_deals", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


class ContactTicketsTable(HubSpotAssociationTable):
    """Association table for contact-ticket relationships."""
    
    FROM_OBJECT_TYPE = "contacts"
    TO_OBJECT_TYPE = "tickets"
    FROM_ID_COLUMN = "contact_id"
    TO_ID_COLUMN = "ticket_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "contact_tickets",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking contacts to their tickets",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "contact_tickets", "COLUMN_NAME": "contact_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Contact ID"},
            {"TABLE_NAME": "contact_tickets", "COLUMN_NAME": "ticket_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Ticket ID"},
            {"TABLE_NAME": "contact_tickets", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "contact_tickets", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


# =============================================================================
# Deal Association Tables
# =============================================================================

class DealCompaniesTable(HubSpotAssociationTable):
    """Association table for deal-company relationships."""
    
    FROM_OBJECT_TYPE = "deals"
    TO_OBJECT_TYPE = "companies"
    FROM_ID_COLUMN = "deal_id"
    TO_ID_COLUMN = "company_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "deal_companies",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking deals to their companies",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "deal_companies", "COLUMN_NAME": "deal_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Deal ID"},
            {"TABLE_NAME": "deal_companies", "COLUMN_NAME": "company_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Company ID"},
            {"TABLE_NAME": "deal_companies", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "deal_companies", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


class DealContactsTable(HubSpotAssociationTable):
    """Association table for deal-contact relationships."""
    
    FROM_OBJECT_TYPE = "deals"
    TO_OBJECT_TYPE = "contacts"
    FROM_ID_COLUMN = "deal_id"
    TO_ID_COLUMN = "contact_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "deal_contacts",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking deals to their contacts",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "deal_contacts", "COLUMN_NAME": "deal_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Deal ID"},
            {"TABLE_NAME": "deal_contacts", "COLUMN_NAME": "contact_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Contact ID"},
            {"TABLE_NAME": "deal_contacts", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "deal_contacts", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


# =============================================================================
# Ticket Association Tables
# =============================================================================

class TicketCompaniesTable(HubSpotAssociationTable):
    """Association table for ticket-company relationships."""
    
    FROM_OBJECT_TYPE = "tickets"
    TO_OBJECT_TYPE = "companies"
    FROM_ID_COLUMN = "ticket_id"
    TO_ID_COLUMN = "company_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "ticket_companies",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking tickets to their companies",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "ticket_companies", "COLUMN_NAME": "ticket_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Ticket ID"},
            {"TABLE_NAME": "ticket_companies", "COLUMN_NAME": "company_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Company ID"},
            {"TABLE_NAME": "ticket_companies", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "ticket_companies", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


class TicketContactsTable(HubSpotAssociationTable):
    """Association table for ticket-contact relationships."""
    
    FROM_OBJECT_TYPE = "tickets"
    TO_OBJECT_TYPE = "contacts"
    FROM_ID_COLUMN = "ticket_id"
    TO_ID_COLUMN = "contact_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "ticket_contacts",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking tickets to their contacts",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "ticket_contacts", "COLUMN_NAME": "ticket_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Ticket ID"},
            {"TABLE_NAME": "ticket_contacts", "COLUMN_NAME": "contact_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Contact ID"},
            {"TABLE_NAME": "ticket_contacts", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "ticket_contacts", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


class TicketDealsTable(HubSpotAssociationTable):
    """Association table for ticket-deal relationships."""
    
    FROM_OBJECT_TYPE = "tickets"
    TO_OBJECT_TYPE = "deals"
    FROM_ID_COLUMN = "ticket_id"
    TO_ID_COLUMN = "deal_id"
    
    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        return {
            "TABLE_NAME": "ticket_deals",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking tickets to their deals",
            "ROW_COUNT": None,
        }
    
    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        return [
            {"TABLE_NAME": "ticket_deals", "COLUMN_NAME": "ticket_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Ticket ID"},
            {"TABLE_NAME": "ticket_deals", "COLUMN_NAME": "deal_id", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Deal ID"},
            {"TABLE_NAME": "ticket_deals", "COLUMN_NAME": "association_type", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association type ID"},
            {"TABLE_NAME": "ticket_deals", "COLUMN_NAME": "association_label", "DATA_TYPE": "VARCHAR", "COLUMN_DESCRIPTION": "Association label"},
        ]


# Export all association table classes
ASSOCIATION_TABLE_CLASSES = {
    "company_contacts": CompanyContactsTable,
    "company_deals": CompanyDealsTable,
    "company_tickets": CompanyTicketsTable,
    "contact_companies": ContactCompaniesTable,
    "contact_deals": ContactDealsTable,
    "contact_tickets": ContactTicketsTable,
    "deal_companies": DealCompaniesTable,
    "deal_contacts": DealContactsTable,
    "ticket_companies": TicketCompaniesTable,
    "ticket_contacts": TicketContactsTable,
    "ticket_deals": TicketDealsTable,
}

"""
Modified DealsTable with primary association columns.

This is a drop-in replacement for the DealsTable class in hubspot_tables.py
that adds primary_company_id and primary_contact_id columns.
"""

from typing import List, Dict, Text, Any, Optional, Set
import pandas as pd
from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.hubspot_tables import (
    HubSpotAPIResource,
    _normalize_filter_conditions,
    _normalize_conditions_for_executor,
    _build_hubspot_properties,
    _build_hubspot_search_filters,
    _execute_hubspot_search,
    to_hubspot_property,
    to_internal_property,
)
from mindsdb.integrations.handlers.hubspot_handler.hubspot_association_utils import (
    get_association_targets_for_object,
    get_primary_association_columns,
    enrich_object_with_associations,
)
from mindsdb.integrations.utilities.handlers.query_utilities import UPDATEQueryExecutor, DELETEQueryExecutor
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn
from mindsdb.utilities import log

from hubspot.crm.objects import (
    SimplePublicObjectId as HubSpotObjectId,
    SimplePublicObjectBatchInput as HubSpotObjectBatchInput,
    SimplePublicObjectInputForCreate as HubSpotObjectInputCreate,
    BatchInputSimplePublicObjectBatchInputForCreate,
    BatchInputSimplePublicObjectBatchInput,
    BatchInputSimplePublicObjectId,
)

logger = log.getLogger(__name__)


class DealsTableWithAssociations(HubSpotAPIResource):
    """
    HubSpot Deals table with primary association columns.
    
    This table includes:
    - All standard deal properties
    - primary_company_id: The first associated company
    - primary_contact_id: The first associated contact
    
    These allow simple JOINs like:
        SELECT * FROM companies c 
        JOIN deals d ON c.id = d.primary_company_id
    """

    SEARCHABLE_COLUMNS = {
        "dealname",
        "amount",
        "dealstage",
        "pipeline",
        "closedate",
        "hubspot_owner_id",
        "closed_won_reason",
        "closed_lost_reason",
        "lead_attribution",
        "services_requested",
        "platform",
        "referral_partner",
        "referral_commission_amount",
        "tech_partners_involved",
        "sales_tier",
        "commission_status",
        "id",
        "lastmodifieddate",
        # Note: primary_company_id and primary_contact_id are NOT searchable
        # via HubSpot Search API - they're derived from associations
    }

    # Association columns that require fetching associations from API
    ASSOCIATION_COLUMNS = {"primary_company_id", "primary_contact_id"}

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
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
    ) -> pd.DataFrame:
        deals_df = pd.json_normalize(
            self.get_deals(
                limit=limit,
                where_conditions=conditions,
                properties=targets,
                search_filters=search_filters,
                search_sorts=search_sorts,
                allow_search=allow_search,
            )
        )
        if deals_df.empty:
            deals_df = pd.DataFrame(columns=targets or self._get_default_deal_columns())
        else:
            deals_df = self._cast_deal_columns(deals_df)
        return deals_df

    def add(self, deal_data: List[dict]):
        self.create_deals(deal_data)

    def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        deals_df = pd.json_normalize(self.get_deals(limit=200, where_conditions=where_conditions))

        if deals_df.empty:
            raise ValueError("No deals retrieved from HubSpot to evaluate update conditions.")

        executor_conditions = _normalize_conditions_for_executor(where_conditions)
        update_query_executor = UPDATEQueryExecutor(deals_df, executor_conditions)
        filtered_df = update_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No deals found matching WHERE conditions: {conditions}.")

        deal_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Updating {len(deal_ids)} deal(s) matching WHERE conditions")
        self.update_deals(deal_ids, values)

    def remove(self, conditions: List[FilterCondition]) -> None:
        where_conditions = _normalize_filter_conditions(conditions)
        deals_df = pd.json_normalize(self.get_deals(limit=200, where_conditions=where_conditions))

        if deals_df.empty:
            raise ValueError("No deals retrieved from HubSpot to evaluate delete conditions.")

        executor_conditions = _normalize_conditions_for_executor(where_conditions)
        delete_query_executor = DELETEQueryExecutor(deals_df, executor_conditions)
        filtered_df = delete_query_executor.execute_query()

        if filtered_df.empty:
            raise ValueError(f"No deals found matching WHERE conditions: {conditions}.")

        deal_ids = filtered_df["id"].astype(str).tolist()
        logger.info(f"Deleting {len(deal_ids)} deal(s) matching WHERE conditions")
        self.delete_deals(deal_ids)

    def get_columns(self) -> List[Text]:
        return self._get_default_deal_columns()

    @staticmethod
    def _get_default_deal_columns() -> List[str]:
        """Return default columns including primary association columns."""
        return [
            "id",
            "dealname",
            "amount",
            "pipeline",
            "closedate",
            "dealstage",
            "hubspot_owner_id",
            "closed_won_reason",
            "closed_lost_reason",
            "lead_attribution",
            "services_requested",
            "platform",
            "referral_partner",
            "referral_commission_amount",
            "tech_partners_involved",
            "sales_tier",
            "commission_status",
            "createdate",
            "lastmodifieddate",
            # Primary association columns (derived from HubSpot associations)
            "primary_company_id",
            "primary_contact_id",
        ]

    @staticmethod
    def _cast_deal_columns(deals_df: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = ["amount"]
        datetime_columns = ["closedate", "createdate", "lastmodifieddate"]
        for column in numeric_columns:
            if column in deals_df.columns:
                deals_df[column] = pd.to_numeric(deals_df[column], errors="coerce")
        for column in datetime_columns:
            if column in deals_df.columns:
                deals_df[column] = pd.to_datetime(deals_df[column], errors="coerce")
        return deals_df

    def _needs_associations(self, columns: List[str]) -> bool:
        """Check if any requested columns require association data."""
        return bool(set(columns) & self.ASSOCIATION_COLUMNS)

    def get_deals(
        self,
        limit: Optional[int] = None,
        where_conditions: Optional[List] = None,
        properties: Optional[List[str]] = None,
        search_filters: Optional[List[Dict[str, Any]]] = None,
        search_sorts: Optional[List[Dict[str, Any]]] = None,
        allow_search: bool = True,
        **kwargs,
    ) -> List[Dict]:
        normalized_conditions = _normalize_filter_conditions(where_conditions)
        hubspot = self.handler.connect()
        
        requested_properties = properties or []
        default_properties = self._get_default_deal_columns()
        columns = requested_properties or default_properties
        
        # Separate HubSpot properties from association columns
        hubspot_columns = [c for c in columns if c not in self.ASSOCIATION_COLUMNS]
        hubspot_properties = _build_hubspot_properties(hubspot_columns)
        
        # Determine if we need to fetch associations
        needs_associations = self._needs_associations(columns)
        association_targets = []
        if needs_associations:
            if "primary_company_id" in columns:
                association_targets.append("companies")
            if "primary_contact_id" in columns:
                association_targets.append("contacts")
        
        api_kwargs = {**kwargs, "properties": hubspot_properties}
        if limit is not None:
            api_kwargs["limit"] = limit
        else:
            api_kwargs.pop("limit", None)
        
        # Add associations to the API call if needed
        if association_targets:
            api_kwargs["associations"] = association_targets
            logger.debug(f"Fetching deals with associations: {association_targets}")

        # Try search API if filters/sorts are provided
        if allow_search and (search_filters or search_sorts or normalized_conditions):
            filters = search_filters
            if filters is None and normalized_conditions:
                # Only build filters for searchable columns
                searchable_conditions = [
                    c for c in normalized_conditions 
                    if c[1] not in self.ASSOCIATION_COLUMNS
                ]
                if searchable_conditions:
                    filters = _build_hubspot_search_filters(searchable_conditions, self.SEARCHABLE_COLUMNS)
            
            if filters is not None or search_sorts is not None:
                # Search API doesn't support associations parameter
                # So we need to fetch associations separately for search results
                search_results = self._search_deals_with_associations(
                    hubspot, filters, hubspot_properties, limit, search_sorts, columns,
                    needs_associations, association_targets
                )
                logger.info(f"Retrieved {len(search_results)} deals from HubSpot via search API")
                return search_results

        # Use get_all API which supports associations
        deals = hubspot.crm.deals.get_all(**api_kwargs)
        deals_dict = []
        for deal in deals:
            try:
                row = self._deal_to_dict(deal, hubspot_columns)
                
                # Enrich with association data if needed
                if needs_associations:
                    row = enrich_object_with_associations(deal, "deals", row)
                
                deals_dict.append(row)
            except Exception as e:
                logger.error(f"Error processing deal {getattr(deal, 'id', 'unknown')}: {str(e)}")
                raise ValueError(f"Failed to process deal {getattr(deal, 'id', 'unknown')}." ) from e

        logger.info(f"Retrieved {len(deals_dict)} deals from HubSpot")
        return deals_dict

    def _search_deals_with_associations(
        self,
        hubspot: HubSpot,
        filters: Optional[List[Dict[str, Any]]],
        properties: List[str],
        limit: Optional[int],
        sorts: Optional[List[Dict[str, Any]]],
        columns: List[str],
        needs_associations: bool,
        association_targets: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Execute search and then enrich results with associations.
        
        The HubSpot Search API doesn't support the associations parameter,
        so we need to fetch associations separately for search results.
        """
        # First, get search results
        hubspot_columns = [c for c in columns if c not in self.ASSOCIATION_COLUMNS]
        search_results = _execute_hubspot_search(
            hubspot.crm.deals.search_api,
            filters or [],
            properties,
            limit,
            lambda obj: self._deal_to_dict(obj, hubspot_columns),
            sorts=sorts,
        )
        
        if not needs_associations or not search_results:
            return search_results
        
        # Fetch associations for all deal IDs in the search results
        deal_ids = [r["id"] for r in search_results if r.get("id")]
        
        if not deal_ids:
            return search_results
        
        # Batch fetch associations using the associations API
        associations_map = self._batch_fetch_associations(hubspot, deal_ids, association_targets)
        
        # Enrich results with associations
        for result in search_results:
            deal_id = str(result.get("id", ""))
            if deal_id in associations_map:
                result.update(associations_map[deal_id])
            else:
                # Set None for association columns not found
                if "primary_company_id" in columns:
                    result.setdefault("primary_company_id", None)
                if "primary_contact_id" in columns:
                    result.setdefault("primary_contact_id", None)
        
        return search_results

    def _batch_fetch_associations(
        self,
        hubspot: HubSpot,
        deal_ids: List[str],
        association_targets: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Batch fetch associations for multiple deals.
        
        Returns a dict mapping deal_id -> {primary_company_id: ..., primary_contact_id: ...}
        """
        associations_map = {}
        
        for target in association_targets:
            try:
                # Use batch read to get associations
                # Reference: https://developers.hubspot.com/docs/api/crm/associations
                from hubspot.crm.associations.v4 import BatchInputPublicFetchAssociationsBatchRequest
                
                batch_input = BatchInputPublicFetchAssociationsBatchRequest(
                    inputs=[{"id": did} for did in deal_ids]
                )
                
                response = hubspot.crm.associations.v4.batch_api.get_page(
                    from_object_type="deals",
                    to_object_type=target,
                    batch_input_public_fetch_associations_batch_request=batch_input,
                )
                
                column_name = f"primary_{target[:-1]}_id"  # companies -> primary_company_id
                
                for result in response.results:
                    deal_id = str(result._from.id)
                    if deal_id not in associations_map:
                        associations_map[deal_id] = {}
                    
                    # Get the first associated object ID
                    if result.to and len(result.to) > 0:
                        associations_map[deal_id][column_name] = str(result.to[0].to_object_id)
                    else:
                        associations_map[deal_id][column_name] = None
                        
            except Exception as e:
                logger.warning(f"Failed to batch fetch {target} associations: {str(e)}")
                # Fall back to None for this association type
                for deal_id in deal_ids:
                    if deal_id not in associations_map:
                        associations_map[deal_id] = {}
                    column_name = f"primary_{target[:-1]}_id"
                    associations_map[deal_id].setdefault(column_name, None)
        
        return associations_map

    def _deal_to_dict(self, deal: Any, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        columns = columns or [c for c in self._get_default_deal_columns() if c not in self.ASSOCIATION_COLUMNS]
        return self._object_to_dict(deal, columns)

    def create_deals(self, deals_data: List[Dict[Text, Any]]) -> None:
        if not deals_data:
            raise ValueError("No deal data provided for creation")

        logger.info(f"Attempting to create {len(deals_data)} deal(s)")
        hubspot = self.handler.connect()
        deals_to_create = [HubSpotObjectInputCreate(properties=deal) for deal in deals_data]
        batch_input = BatchInputSimplePublicObjectBatchInputForCreate(inputs=deals_to_create)

        try:
            created_deals = hubspot.crm.deals.batch_api.create(
                batch_input_simple_public_object_batch_input_for_create=batch_input
            )
            if not created_deals or not hasattr(created_deals, "results") or not created_deals.results:
                raise Exception("Deal creation returned no results")
            created_ids = [d.id for d in created_deals.results]
            logger.info(f"Successfully created {len(created_ids)} deal(s) with IDs: {created_ids}")
        except Exception as e:
            logger.error(f"Deals creation failed: {str(e)}")
            raise Exception(f"Deals creation failed {e}")

    def update_deals(self, deal_ids: List[Text], values_to_update: Dict[Text, Any]) -> None:
        hubspot = self.handler.connect()
        deals_to_update = [HubSpotObjectBatchInput(id=did, properties=values_to_update) for did in deal_ids]
        batch_input = BatchInputSimplePublicObjectBatchInput(inputs=deals_to_update)
        try:
            updated = hubspot.crm.deals.batch_api.update(batch_input_simple_public_object_batch_input=batch_input)
            logger.info(f"Deals with ID {[d.id for d in updated.results]} updated")
        except Exception as e:
            raise Exception(f"Deals update failed {e}")

    def delete_deals(self, deal_ids: List[Text]) -> None:
        hubspot = self.handler.connect()
        deals_to_delete = [HubSpotObjectId(id=did) for did in deal_ids]
        batch_input = BatchInputSimplePublicObjectId(inputs=deals_to_delete)
        try:
            hubspot.crm.deals.batch_api.archive(batch_input_simple_public_object_id=batch_input)
            logger.info("Deals deleted")
        except Exception as e:
            raise Exception(f"Deals deletion failed {e}")
