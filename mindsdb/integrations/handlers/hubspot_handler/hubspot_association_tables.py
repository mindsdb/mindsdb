"""
HubSpot Association Tables for MindsDB.

This module provides association tables that expose the many-to-many relationships
between HubSpot CRM objects (companies, contacts, deals, tickets, etc.).

Reference: https://developers.hubspot.com/docs/api/crm/associations
"""
from __future__ import annotations

from typing import Any
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn
from mindsdb.utilities import log

logger = log.getLogger(__name__)


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

    def get_columns(self) -> list[str]:
        """Return column names for the association table."""
        return [self.FROM_ID_COLUMN, self.TO_ID_COLUMN, "association_type", "association_label"]

    def select(self, query) -> pd.DataFrame:
        """Execute SELECT query on association table."""
        result_limit = query.limit.value if query.limit else None

        return self.list(limit=result_limit)

    def list(
        self,
        conditions: list[FilterCondition] | None = None,
        limit: int | None = None,
        sort: list[SortColumn] | None = None,
        targets: list[str] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch associations between objects."""
        associations = self._fetch_associations(limit=limit)

        if not associations:
            return pd.DataFrame(columns=self.get_columns())

        df = pd.DataFrame(associations)

        if conditions:
            df = self._apply_conditions(df, conditions)

        return df

    def _fetch_associations(self, limit: int | None = None) -> list[dict[str, Any]]:
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
                source_objects = getattr(hubspot.crm, self.FROM_OBJECT_TYPE).get_all(
                    associations=[self.TO_OBJECT_TYPE],
                    limit=limit or 500,
                )
            else:
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

                    assoc_type = None
                    assoc_label = None
                    assoc_category = None

                    if hasattr(assoc, "type"):
                        assoc_type = assoc.type
                    if hasattr(assoc, "category"):
                        assoc_category = assoc.category
                    elif isinstance(assoc, dict):
                        assoc_type = assoc.get("type")
                        assoc_category = assoc.get("category")

                    if hasattr(assoc, "associationTypes") and assoc.associationTypes:
                        for at in assoc.associationTypes:
                            assoc_label = getattr(at, "label", assoc_label)
                            assoc_type = getattr(at, "typeId", assoc_type)
                            assoc_category = getattr(at, "category", assoc_category)
                            break
                    elif isinstance(assoc, dict):
                        assoc_types = assoc.get("associationTypes") or assoc.get("association_types") or []
                        if assoc_types:
                            assoc_entry = assoc_types[0]
                            if isinstance(assoc_entry, dict):
                                assoc_label = assoc_label or assoc_entry.get("label")
                                assoc_type = assoc_type or assoc_entry.get("typeId") or assoc_entry.get("type")
                                assoc_category = assoc_category or assoc_entry.get("category")

                    if not assoc_label and assoc_type is not None:
                        fallback_category = assoc_category or "HUBSPOT_DEFINED"
                        assoc_label = f"{fallback_category}:{assoc_type}"

                    results.append(
                        {
                            self.FROM_ID_COLUMN: str(from_id),
                            self.TO_ID_COLUMN: str(to_id),
                            "association_type": assoc_type,
                            "association_label": assoc_label,
                        }
                    )

                if limit and len(results) >= limit:
                    break

            logger.info(f"Retrieved {len(results)} {self.FROM_OBJECT_TYPE}->{self.TO_OBJECT_TYPE} associations")
            return results

        except Exception as e:
            logger.error(f"Failed to fetch associations: {str(e)}")
            raise

    def _apply_conditions(self, df: pd.DataFrame, conditions: list[FilterCondition]) -> pd.DataFrame:
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

    def add(self, data: list[dict]) -> None:
        """Create associations - not yet implemented."""
        raise NotImplementedError(
            "Creating associations via INSERT is not yet supported. "
            "Use the HubSpot API directly to create associations."
        )

    def modify(self, conditions: list[FilterCondition], values: dict) -> None:
        """Update associations - not applicable."""
        raise NotImplementedError("Associations cannot be updated. Delete and recreate instead.")

    def remove(self, conditions: list[FilterCondition]) -> None:
        """Delete associations - not yet implemented."""
        raise NotImplementedError(
            "Deleting associations via DELETE is not yet supported. "
            "Use the HubSpot API directly to remove associations."
        )


class CompanyContactsTable(HubSpotAssociationTable):
    """Association table for company-contact relationships."""

    FROM_OBJECT_TYPE = "companies"
    TO_OBJECT_TYPE = "contacts"
    FROM_ID_COLUMN = "company_id"
    TO_ID_COLUMN = "contact_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "company_contacts",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking companies to their contacts",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "company_contacts",
                "COLUMN_NAME": "company_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Company ID",
            },
            {
                "TABLE_NAME": "company_contacts",
                "COLUMN_NAME": "contact_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Contact ID",
            },
            {
                "TABLE_NAME": "company_contacts",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "company_contacts",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class CompanyDealsTable(HubSpotAssociationTable):
    """Association table for company-deal relationships."""

    FROM_OBJECT_TYPE = "companies"
    TO_OBJECT_TYPE = "deals"
    FROM_ID_COLUMN = "company_id"
    TO_ID_COLUMN = "deal_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "company_deals",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking companies to their deals",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "company_deals",
                "COLUMN_NAME": "company_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Company ID",
            },
            {
                "TABLE_NAME": "company_deals",
                "COLUMN_NAME": "deal_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Deal ID",
            },
            {
                "TABLE_NAME": "company_deals",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "company_deals",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class CompanyTicketsTable(HubSpotAssociationTable):
    """Association table for company-ticket relationships."""

    FROM_OBJECT_TYPE = "companies"
    TO_OBJECT_TYPE = "tickets"
    FROM_ID_COLUMN = "company_id"
    TO_ID_COLUMN = "ticket_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "company_tickets",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking companies to their tickets",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "company_tickets",
                "COLUMN_NAME": "company_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Company ID",
            },
            {
                "TABLE_NAME": "company_tickets",
                "COLUMN_NAME": "ticket_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Ticket ID",
            },
            {
                "TABLE_NAME": "company_tickets",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "company_tickets",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class ContactCompaniesTable(HubSpotAssociationTable):
    """Association table for contact-company relationships."""

    FROM_OBJECT_TYPE = "contacts"
    TO_OBJECT_TYPE = "companies"
    FROM_ID_COLUMN = "contact_id"
    TO_ID_COLUMN = "company_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "contact_companies",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking contacts to their companies",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "contact_companies",
                "COLUMN_NAME": "contact_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Contact ID",
            },
            {
                "TABLE_NAME": "contact_companies",
                "COLUMN_NAME": "company_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Company ID",
            },
            {
                "TABLE_NAME": "contact_companies",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "contact_companies",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class ContactDealsTable(HubSpotAssociationTable):
    """Association table for contact-deal relationships."""

    FROM_OBJECT_TYPE = "contacts"
    TO_OBJECT_TYPE = "deals"
    FROM_ID_COLUMN = "contact_id"
    TO_ID_COLUMN = "deal_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "contact_deals",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking contacts to their deals",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "contact_deals",
                "COLUMN_NAME": "contact_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Contact ID",
            },
            {
                "TABLE_NAME": "contact_deals",
                "COLUMN_NAME": "deal_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Deal ID",
            },
            {
                "TABLE_NAME": "contact_deals",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "contact_deals",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class ContactTicketsTable(HubSpotAssociationTable):
    """Association table for contact-ticket relationships."""

    FROM_OBJECT_TYPE = "contacts"
    TO_OBJECT_TYPE = "tickets"
    FROM_ID_COLUMN = "contact_id"
    TO_ID_COLUMN = "ticket_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "contact_tickets",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking contacts to their tickets",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "contact_tickets",
                "COLUMN_NAME": "contact_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Contact ID",
            },
            {
                "TABLE_NAME": "contact_tickets",
                "COLUMN_NAME": "ticket_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Ticket ID",
            },
            {
                "TABLE_NAME": "contact_tickets",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "contact_tickets",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class DealCompaniesTable(HubSpotAssociationTable):
    """Association table for deal-company relationships."""

    FROM_OBJECT_TYPE = "deals"
    TO_OBJECT_TYPE = "companies"
    FROM_ID_COLUMN = "deal_id"
    TO_ID_COLUMN = "company_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "deal_companies",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking deals to their companies",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "deal_companies",
                "COLUMN_NAME": "deal_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Deal ID",
            },
            {
                "TABLE_NAME": "deal_companies",
                "COLUMN_NAME": "company_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Company ID",
            },
            {
                "TABLE_NAME": "deal_companies",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "deal_companies",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class DealContactsTable(HubSpotAssociationTable):
    """Association table for deal-contact relationships."""

    FROM_OBJECT_TYPE = "deals"
    TO_OBJECT_TYPE = "contacts"
    FROM_ID_COLUMN = "deal_id"
    TO_ID_COLUMN = "contact_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "deal_contacts",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking deals to their contacts",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "deal_contacts",
                "COLUMN_NAME": "deal_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Deal ID",
            },
            {
                "TABLE_NAME": "deal_contacts",
                "COLUMN_NAME": "contact_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Contact ID",
            },
            {
                "TABLE_NAME": "deal_contacts",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "deal_contacts",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class TicketCompaniesTable(HubSpotAssociationTable):
    """Association table for ticket-company relationships."""

    FROM_OBJECT_TYPE = "tickets"
    TO_OBJECT_TYPE = "companies"
    FROM_ID_COLUMN = "ticket_id"
    TO_ID_COLUMN = "company_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "ticket_companies",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking tickets to their companies",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "ticket_companies",
                "COLUMN_NAME": "ticket_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Ticket ID",
            },
            {
                "TABLE_NAME": "ticket_companies",
                "COLUMN_NAME": "company_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Company ID",
            },
            {
                "TABLE_NAME": "ticket_companies",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "ticket_companies",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class TicketContactsTable(HubSpotAssociationTable):
    """Association table for ticket-contact relationships."""

    FROM_OBJECT_TYPE = "tickets"
    TO_OBJECT_TYPE = "contacts"
    FROM_ID_COLUMN = "ticket_id"
    TO_ID_COLUMN = "contact_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "ticket_contacts",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking tickets to their contacts",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "ticket_contacts",
                "COLUMN_NAME": "ticket_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Ticket ID",
            },
            {
                "TABLE_NAME": "ticket_contacts",
                "COLUMN_NAME": "contact_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Contact ID",
            },
            {
                "TABLE_NAME": "ticket_contacts",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "ticket_contacts",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
        ]


class TicketDealsTable(HubSpotAssociationTable):
    """Association table for ticket-deal relationships."""

    FROM_OBJECT_TYPE = "tickets"
    TO_OBJECT_TYPE = "deals"
    FROM_ID_COLUMN = "ticket_id"
    TO_ID_COLUMN = "deal_id"

    def meta_get_tables(self, table_name: str) -> dict[str, Any]:
        return {
            "TABLE_NAME": "ticket_deals",
            "TABLE_TYPE": "VIEW",
            "TABLE_DESCRIPTION": "Association table linking tickets to their deals",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {
                "TABLE_NAME": "ticket_deals",
                "COLUMN_NAME": "ticket_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Ticket ID",
            },
            {
                "TABLE_NAME": "ticket_deals",
                "COLUMN_NAME": "deal_id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Deal ID",
            },
            {
                "TABLE_NAME": "ticket_deals",
                "COLUMN_NAME": "association_type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association type ID",
            },
            {
                "TABLE_NAME": "ticket_deals",
                "COLUMN_NAME": "association_label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Association label",
            },
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
