"""HubSpot association utilities for MindsDB."""

from typing import Any, Dict, List, Optional


PRIMARY_ASSOCIATIONS_CONFIG = {
    "companies": [],
    "contacts": [
        ("companies", "primary_company_id"),
    ],
    "deals": [
        ("companies", "primary_company_id"),
        ("contacts", "primary_contact_id"),
    ],
    "tickets": [
        ("companies", "primary_company_id"),
        ("contacts", "primary_contact_id"),
        ("deals", "primary_deal_id"),
    ],
    "tasks": [
        ("contacts", "primary_contact_id"),
        ("companies", "primary_company_id"),
        ("deals", "primary_deal_id"),
    ],
    "calls": [
        ("contacts", "primary_contact_id"),
        ("companies", "primary_company_id"),
        ("deals", "primary_deal_id"),
    ],
    "emails": [
        ("contacts", "primary_contact_id"),
        ("companies", "primary_company_id"),
        ("deals", "primary_deal_id"),
    ],
    "meetings": [
        ("contacts", "primary_contact_id"),
        ("companies", "primary_company_id"),
        ("deals", "primary_deal_id"),
    ],
    "notes": [
        ("contacts", "primary_contact_id"),
        ("companies", "primary_company_id"),
        ("deals", "primary_deal_id"),
    ],
}


def extract_primary_association(obj: Any, to_object_type: str) -> Optional[str]:
    associations = getattr(obj, "associations", None)
    if not associations:
        return None

    if isinstance(associations, dict):
        to_objects = associations.get(to_object_type, {})
        if isinstance(to_objects, dict):
            results = to_objects.get("results", [])
        else:
            results = to_objects if isinstance(to_objects, list) else []
    else:
        to_objects = getattr(associations, to_object_type, None)
        if to_objects is None:
            return None
        results = getattr(to_objects, "results", []) or []

    if not results:
        return None

    first_assoc = results[0]
    if hasattr(first_assoc, "id"):
        return str(first_assoc.id)
    if isinstance(first_assoc, dict) and first_assoc.get("id"):
        return str(first_assoc["id"])
    if hasattr(first_assoc, "toObjectId"):
        return str(first_assoc.toObjectId)

    return None


def get_association_targets_for_object(object_type: str) -> List[str]:
    config = PRIMARY_ASSOCIATIONS_CONFIG.get(object_type, [])
    return [target for target, _ in config]


def get_primary_association_columns(object_type: str) -> List[str]:
    config = PRIMARY_ASSOCIATIONS_CONFIG.get(object_type, [])
    return [col_name for _, col_name in config]


def enrich_object_with_associations(obj: Any, object_type: str, row: Dict[str, Any]) -> Dict[str, Any]:
    config = PRIMARY_ASSOCIATIONS_CONFIG.get(object_type, [])
    for target_type, column_name in config:
        row[column_name] = extract_primary_association(obj, target_type)
    return row


def extract_association_rows(
    obj: Any, from_column: str, to_column: str, to_object_type: str
) -> List[Dict[str, Any]]:
    from_id = getattr(obj, "id", None)
    if from_id is None:
        return []

    associations = getattr(obj, "associations", None)
    if not associations:
        return []

    if isinstance(associations, dict):
        to_objects = associations.get(to_object_type, {})
        if isinstance(to_objects, dict):
            results = to_objects.get("results", [])
        else:
            results = to_objects if isinstance(to_objects, list) else []
    else:
        to_objects = getattr(associations, to_object_type, None)
        results = getattr(to_objects, "results", []) if to_objects else []

    rows = []
    for assoc in results or []:
        assoc_id = getattr(assoc, "id", None)
        if assoc_id is None and isinstance(assoc, dict):
            assoc_id = assoc.get("id")
        if assoc_id is None:
            continue

        assoc_type = None
        assoc_label = None
        if isinstance(assoc, dict):
            assoc_type = assoc.get("type") or assoc.get("typeId")
            assoc_label = assoc.get("label")
            assoc_types = assoc.get("associationTypes") or assoc.get("association_types")
            if assoc_types:
                assoc_entry = assoc_types[0]
                if isinstance(assoc_entry, dict):
                    assoc_type = assoc_type or assoc_entry.get("typeId") or assoc_entry.get("type")
                    assoc_label = assoc_label or assoc_entry.get("label")
        else:
            assoc_type = getattr(assoc, "type", None)
            assoc_label = getattr(assoc, "label", None)
            assoc_types = getattr(assoc, "associationTypes", None) or getattr(assoc, "association_types", None)
            if assoc_types:
                assoc_entry = assoc_types[0]
                assoc_type = assoc_type or getattr(assoc_entry, "typeId", None) or getattr(assoc_entry, "type", None)
                assoc_label = assoc_label or getattr(assoc_entry, "label", None)

        rows.append(
            {
                from_column: str(from_id),
                to_column: str(assoc_id),
                "association_type": assoc_type,
                "association_label": assoc_label,
            }
        )
    return rows
