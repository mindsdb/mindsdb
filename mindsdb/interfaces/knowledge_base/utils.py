"""Utilities for knowledge base operations."""
import hashlib


def generate_document_id(content: str, content_column: str = None, provided_id: str = None) -> str:
    """
    Generate a deterministic document ID from content.
    If provided_id exists, returns it directly.
    For generated IDs, uses a short hash of just the content.

    Args:
        content: The content string
        content_column: Name of the content column (not used in ID generation, kept for backward compatibility)
        provided_id: Optional user-provided ID
    Returns:
        Deterministic document ID (either provided_id or a 16-char hash of content)
    """
    if provided_id is not None:
        return provided_id

    # Generate a shorter 16-character hash based only on content
    hash_obj = hashlib.md5(content.encode())
    return hash_obj.hexdigest()[:16]
