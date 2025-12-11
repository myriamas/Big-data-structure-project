"""
Storage size computation module for NoSQL collections.

This module provides functions to calculate the approximate storage size
of JSON documents based on their schema, following standard NoSQL sizing rules.

Sizing Rules:
- Integer/Number: 8 bytes
- String: 80 bytes (average)
- Date: 20 bytes
- LongString (description, comment): 200 bytes
- Key+Value overhead: 12 bytes per field
- Arrays: 2 elements on average

The module uses recursive computation to handle nested objects and arrays.
"""

TYPE_SIZES = {
    "integer": 8,       # 64-bit integer
    "number": 8,        # 64-bit float
    "string": 80,       # Average string length
    "date": 20,         # Date string representation
    "longstring": 200,  # Long textual fields
}

KEY_OVERHEAD = 12  # Overhead per key/value pair or array field


def compute_document_size_from_schema(schema):
    """
    I compute the approximate size of one document described by a JSON Schema.

    I use the type information and a fixed overhead for each key/value pair or array.
    For strings, I also adapt the logical type based on the field name:
      - fields ending with "date" are considered as "date" strings,
      - some textual fields like "description" or "comment" are considered "longstring".
    """
    properties = schema.get("properties", {})
    total_size = 0

    for field_name, field_schema in properties.items():
        field_type = field_schema.get("type")

        if field_type == "object":
            nested_size = compute_document_size_from_schema(field_schema)
            total_size += KEY_OVERHEAD + nested_size

        elif field_type == "array":
            item_schema = field_schema.get("items", {})
            # I assume 2 elements on average for arrays (as suggested in the instructions)
            avg_array_length = 2
            item_size = compute_document_size_from_schema(item_schema)
            total_size += KEY_OVERHEAD + avg_array_length * item_size

        else:
            # I start from the JSON Schema type...
            logical_type = field_type

            # ...and I refine it based on the field name when needed.
            if isinstance(field_name, str) and field_name.lower().endswith("date"):
                # I treat fields like "date" or "deliveryDate" as "date"
                logical_type = "date"

            if field_name in ("description", "comment"):
                # I treat long textual fields as "longstring"
                logical_type = "longstring"

            value_size = TYPE_SIZES.get(logical_type, 0)
            total_size += KEY_OVERHEAD + value_size

    return total_size


def compute_collection_size_bytes(collection):
    """
    I compute the total size in bytes for a whole collection
    given its schema and the number of documents.
    """
    doc_size = compute_document_size_from_schema(collection.schema)
    return doc_size * collection.document_count


def bytes_to_gb(size_bytes):
    """
    I convert a size in bytes to gigabytes.
    """
    return size_bytes / (1024 ** 3)


def compute_database_size_bytes(collections):
    """
    I compute the total size in bytes of a database,
    as the sum of the sizes of all its collections.
    """
    return sum(compute_collection_size_bytes(c) for c in collections)


def compute_database_size_gb(collections):
    """
    I compute the total size in GB of a database from its collections.
    """
    total_bytes = compute_database_size_bytes(collections)
    return bytes_to_gb(total_bytes)
