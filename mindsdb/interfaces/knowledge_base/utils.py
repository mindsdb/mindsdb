"""Utilities for knowledge base operations."""
import hashlib


def generate_document_id(content: str, content_column: str, provided_id: str = None) -> str:
    """
    Generate a deterministic document ID from content and column name.
    If provided_id exists, combines it with content_column.
    For generated IDs, uses a short hash of just the content to ensure
    same content gets same base ID across different columns.

    Args:
        content: The content string
        content_column: Name of the content column
        provided_id: Optional user-provided ID
    Returns:
        Deterministic document ID in format: <base_id>_<column>
        where base_id is either the provided_id or a 16-char hash of content
    """
    if provided_id is not None:
        base_id = provided_id
    else:
        # Generate a shorter 16-character hash based only on content
        hash_obj = hashlib.md5(content.encode())
        base_id = hash_obj.hexdigest()[:16]

    # Append column name to maintain uniqueness across columns
    return f"{base_id}_{content_column}"
