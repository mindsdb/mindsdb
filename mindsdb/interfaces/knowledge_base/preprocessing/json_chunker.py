from typing import List, Dict, Any, Optional
import json
import pandas as pd
import ast

from mindsdb.interfaces.knowledge_base.preprocessing.models import (
    Document,
    ProcessedChunk,
    JSONChunkingConfig
)
from mindsdb.interfaces.knowledge_base.preprocessing.document_preprocessor import DocumentPreprocessor
from mindsdb.utilities import log

# Set up logger
logger = log.getLogger(__name__)


class JSONChunkingPreprocessor(DocumentPreprocessor):
    """JSON chunking preprocessor for handling JSON data structures"""

    def __init__(self, config: Optional[JSONChunkingConfig] = None):
        """Initialize with JSON chunking configuration"""
        super().__init__()
        self.config = config or JSONChunkingConfig()
        # No need for a text splitter here as we'll chunk by JSON structure

    def process_documents(self, documents: List[Document]) -> List[ProcessedChunk]:
        """Process JSON documents into chunks

        Args:
            documents: List of documents containing JSON content

        Returns:
            List of processed chunks
        """
        all_chunks = []

        for doc in documents:
            try:
                # Parse document content into a Python object
                json_data = self._parse_document_content(doc)
                if json_data is None:
                    # Handle parsing failure
                    error_message = "Content is neither valid JSON nor a valid Python literal."
                    error_chunk = self._create_error_chunk(doc, error_message)
                    all_chunks.append(error_chunk)
                    continue  # Skip to next document

                # Process the JSON data based on its structure
                chunks = self._process_json_data(json_data, doc)
                all_chunks.extend(chunks)
            except Exception as e:
                logger.error(f"Error processing document {doc.id}: {e}")
                error_chunk = self._create_error_chunk(doc, str(e))
                all_chunks.append(error_chunk)

        return all_chunks

    def _parse_document_content(self, doc: Document) -> Optional[Any]:
        """Parse document content into a Python object

        Args:
            doc: Document with content to parse

        Returns:
            Parsed content as a Python object or None if parsing failed
        """
        # If content is not a string, assume it's already a Python object
        if not isinstance(doc.content, str):
            return doc.content

        # Try to parse as JSON first
        try:
            return json.loads(doc.content)
        except json.JSONDecodeError:
            # If JSON parsing fails, try as Python literal
            try:
                return ast.literal_eval(doc.content)
            except (SyntaxError, ValueError) as e:
                logger.error(f"Error parsing content for document {doc.id}: {e}")
                # We'll create the error chunk in the main process_documents method
                return None

    def _process_json_data(self, json_data: Any, doc: Document) -> List[ProcessedChunk]:
        """Process JSON data based on its structure

        Args:
            json_data: Parsed JSON data as a Python object
            doc: Original document

        Returns:
            List of processed chunks
        """
        if isinstance(json_data, list):
            # List of objects - chunk by object
            return self._process_json_list(json_data, doc)
        elif isinstance(json_data, dict):
            # Single object - chunk according to config
            if self.config.chunk_by_object:
                return [self._create_chunk_from_dict(json_data, doc, 0, 1)]
            else:
                return self._process_json_dict(json_data, doc)
        else:
            # Primitive value - create a single chunk
            return [self._create_chunk_from_primitive(json_data, doc)]

    def _create_error_chunk(self, doc: Document, error_message: str) -> ProcessedChunk:
        """Create a chunk containing error information

        Args:
            doc: Original document
            error_message: Error message to include in the chunk

        Returns:
            ProcessedChunk with error information
        """
        return ProcessedChunk(
            id=f"{doc.id}_error",
            content=f"Error processing document: {error_message}",
            metadata=self._prepare_chunk_metadata(doc.id, 0, doc.metadata)
        )

    def _process_json_list(self, json_list: List, doc: Document) -> List[ProcessedChunk]:
        """Process a list of JSON objects into chunks"""
        chunks = []
        total_objects = len(json_list)

        for i, item in enumerate(json_list):
            if isinstance(item, dict):
                chunk = self._create_chunk_from_dict(item, doc, i, total_objects)
                chunks.append(chunk)
            elif isinstance(item, list):
                # Handle nested lists by converting to string representation
                chunk = self._create_chunk_from_primitive(
                    json.dumps(item),
                    doc,
                    chunk_index=i,
                    total_chunks=total_objects
                )
                chunks.append(chunk)
            else:
                # Handle primitive values
                chunk = self._create_chunk_from_primitive(
                    item,
                    doc,
                    chunk_index=i,
                    total_chunks=total_objects
                )
                chunks.append(chunk)

        return chunks

    def _process_json_dict(self, json_dict: Dict, doc: Document) -> List[ProcessedChunk]:
        """Process a single JSON object into chunks by fields"""
        chunks = []

        # Ensure we're working with a dictionary
        if isinstance(json_dict, str):
            try:
                json_dict = json.loads(json_dict)
            except json.JSONDecodeError:
                logger.error(f"Error parsing JSON string: {json_dict[:100]}...")
                return [self._create_error_chunk(doc, "Invalid JSON string")]

        # Filter fields based on include/exclude lists
        fields_to_process = {}
        for key, value in json_dict.items():
            if self.config.include_fields and key not in self.config.include_fields:
                continue
            if key in self.config.exclude_fields:
                continue
            fields_to_process[key] = value

        # Create a chunk for each field
        total_fields = len(fields_to_process)
        for i, (key, value) in enumerate(fields_to_process.items()):
            field_content = self._format_field_content(key, value)

            # Create chunk metadata
            metadata = self._prepare_chunk_metadata(doc.id, i, doc.metadata)
            metadata["field_name"] = key

            # Extract fields to metadata for filtering
            self._extract_fields_to_metadata(json_dict, metadata)

            # Generate chunk ID
            chunk_id = self._generate_chunk_id(
                chunk_index=i,
                total_chunks=total_fields,
                start_char=0,
                end_char=len(field_content),
                provided_id=doc.id,
                content_column=self.config.content_column
            )

            # Create and add the chunk
            chunk = ProcessedChunk(
                id=chunk_id,
                content=field_content,
                metadata=metadata
            )
            chunks.append(chunk)

        return chunks

    def _create_chunk_from_dict(self,
                                json_dict: Dict,
                                doc: Document,
                                chunk_index: int,
                                total_chunks: int) -> ProcessedChunk:
        """Create a chunk from a JSON dictionary"""
        # Ensure we're working with a dictionary
        if isinstance(json_dict, str):
            try:
                json_dict = json.loads(json_dict)
            except json.JSONDecodeError:
                logger.error(f"Error parsing JSON string: {json_dict[:100]}...")
                return self._create_error_chunk(doc, "Invalid JSON string")

        # Format the content
        if self.config.flatten_nested:
            flattened = self._flatten_dict(json_dict, self.config.nested_delimiter)
            filtered_dict = self._filter_fields(flattened)
            content = self._dict_to_text(filtered_dict)
        else:
            filtered_dict = {k: v for k, v in json_dict.items()
                             if (not self.config.include_fields or k in self.config.include_fields)
                             and k not in self.config.exclude_fields}
            content = json.dumps(filtered_dict, indent=2)

        # Create metadata
        metadata = self._prepare_chunk_metadata(doc.id, chunk_index, doc.metadata)

        # Extract fields to metadata for filtering
        self._extract_fields_to_metadata(json_dict, metadata)

        # Generate chunk ID
        chunk_id = self._generate_chunk_id(
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            start_char=0,
            end_char=len(content),
            provided_id=doc.id,
            content_column=self.config.content_column
        )

        return ProcessedChunk(
            id=chunk_id,
            content=content,
            metadata=metadata
        )

    def _filter_fields(self, flattened_dict: Dict) -> Dict:
        """Filter fields based on include/exclude configuration"""
        # If include_fields is specified, only keep those fields
        if self.config.include_fields:
            filtered_dict = {k: v for k, v in flattened_dict.items()
                             if any(k == field or k.startswith(field + self.config.nested_delimiter)
                                    for field in self.config.include_fields)}
        else:
            filtered_dict = flattened_dict.copy()

        # Apply exclude_fields
        if self.config.exclude_fields:
            for exclude_field in self.config.exclude_fields:
                # Remove exact field match
                if exclude_field in filtered_dict:
                    filtered_dict.pop(exclude_field)

                # Remove any nested fields
                nested_prefix = exclude_field + self.config.nested_delimiter
                keys_to_remove = [k for k in filtered_dict if k.startswith(nested_prefix)]
                for key in keys_to_remove:
                    filtered_dict.pop(key)

        return filtered_dict

    def _create_chunk_from_primitive(
            self,
            value: Any,
            doc: Document,
            chunk_index: int = 0,
            total_chunks: int = 1
    ) -> ProcessedChunk:
        """Create a chunk from a primitive value"""
        content = str(value)

        # Create metadata
        metadata = self._prepare_chunk_metadata(doc.id, chunk_index, doc.metadata)

        # For primitive values, we don't have a JSON dictionary to extract fields from
        # But we can add the value itself as a metadata field if configured
        if self.config.extract_all_primitives:
            metadata["field_value"] = value

        # Generate chunk ID
        chunk_id = self._generate_chunk_id(
            chunk_index=chunk_index,
            total_chunks=total_chunks,
            start_char=0,
            end_char=len(content),
            provided_id=doc.id,
            content_column=self.config.content_column
        )

        return ProcessedChunk(
            id=chunk_id,
            content=content,
            metadata=metadata
        )

    def _flatten_dict(self, d: Dict, delimiter: str = '.', prefix: str = '') -> Dict:
        """Flatten a nested dictionary structure"""
        result = {}
        for k, v in d.items():
            new_key = f"{prefix}{delimiter}{k}" if prefix else k
            if isinstance(v, dict):
                result.update(self._flatten_dict(v, delimiter, new_key))
            elif isinstance(v, list) and all(isinstance(item, dict) for item in v):
                # Handle lists of dictionaries
                for i, item in enumerate(v):
                    result.update(self._flatten_dict(item, delimiter, f"{new_key}[{i}]"))
            else:
                result[new_key] = v
        return result

    def _dict_to_text(self, d: Dict) -> str:
        """Convert a dictionary to a human-readable text format"""
        lines = []
        for key, value in d.items():
            if value is None:
                continue
            if isinstance(value, list):
                if not value:
                    continue
                if all(isinstance(item, dict) for item in value):
                    # Format list of dictionaries
                    lines.append(f"{key}:")
                    for i, item in enumerate(value):
                        lines.append(f"  Item {i+1}:")
                        for k, v in item.items():
                            lines.append(f"    {k}: {v}")
                else:
                    # Format list of primitives
                    value_str = ", ".join(str(item) for item in value)
                    lines.append(f"{key}: {value_str}")
            else:
                lines.append(f"{key}: {value}")

        return "\n".join(lines)

    def _format_field_content(self, key: str, value: Any) -> str:
        """Format a field's content for inclusion in a chunk"""
        if isinstance(value, dict):
            if self.config.flatten_nested:
                flattened = self._flatten_dict(value, self.config.nested_delimiter, key)
                return self._dict_to_text(flattened)
            else:
                return f"{key}: {json.dumps(value, indent=2)}"
        elif isinstance(value, list):
            if all(isinstance(item, dict) for item in value):
                # Format list of dictionaries
                lines = [f"{key}:"]
                for i, item in enumerate(value):
                    lines.append(f"  Item {i+1}:")
                    for k, v in item.items():
                        lines.append(f"    {k}: {v}")
                return "\n".join(lines)
            else:
                # Format list of primitives
                value_str = ", ".join(str(item) for item in value if item is not None)
                return f"{key}: {value_str}"
        else:
            return f"{key}: {value}"

    def _extract_fields_to_metadata(self, json_dict: Dict, metadata: Dict) -> None:
        """Extract specified fields from JSON to metadata for filtering"""
        # Ensure we're working with a dictionary
        if isinstance(json_dict, str):
            try:
                json_dict = json.loads(json_dict)
            except json.JSONDecodeError:
                logger.error(f"Error parsing JSON string: {json_dict[:100]}...")
                return

        # Always flatten the dictionary for metadata extraction
        flattened = self._flatten_dict(json_dict, self.config.nested_delimiter)

        # If extract_all_primitives is True, extract all primitive values
        if self.config.extract_all_primitives:
            for key, value in flattened.items():
                if isinstance(value, (str, int, float, bool)) and value is not None:
                    metadata[f"field_{key}"] = value
            return

        # If metadata_fields is empty and extract_all_primitives is False,
        # assume all fields should be extracted
        if not self.config.metadata_fields:
            # First try to extract top-level primitive fields
            has_primitives = False
            for key, value in json_dict.items():
                if isinstance(value, (str, int, float, bool)) and value is not None:
                    metadata[f"field_{key}"] = value
                    has_primitives = True

            # If no top-level primitives were found, extract all primitives from flattened dict
            if not has_primitives:
                for key, value in flattened.items():
                    if isinstance(value, (str, int, float, bool)) and value is not None:
                        metadata[f"field_{key}"] = value
        else:
            # Extract only the specified fields
            for field in self.config.metadata_fields:
                if field in flattened and flattened[field] is not None:
                    metadata[f"field_{field}"] = flattened[field]
                else:
                    # Try to navigate the nested structure manually
                    parts = field.split(self.config.nested_delimiter)
                    current = json_dict
                    found = True

                    for part in parts:
                        if isinstance(current, dict) and part in current:
                            current = current[part]
                        else:
                            found = False
                            break

                    if found and current is not None:
                        metadata[f"field_{field}"] = current

    def to_dataframe(self, chunks: List[ProcessedChunk]) -> pd.DataFrame:
        """Convert processed chunks to dataframe format"""
        return pd.DataFrame([chunk.model_dump() for chunk in chunks])
