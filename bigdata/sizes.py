TYPE_SIZES = {
    "integer": 8,
    "number": 8,
    "string": 80,
    "date": 20,
    "longstring": 200
}

KEY_OVERHEAD = 12


def compute_document_size_from_schema(schema):
    properties = schema.get("properties", {})
    total_size = 0

    for field_name, field_schema in properties.items():
        field_type = field_schema.get("type")

        if field_type == "object":
            nested_size = compute_document_size_from_schema(field_schema)
            total_size += KEY_OVERHEAD + nested_size

        elif field_type == "array":
            item_schema = field_schema.get("items", {})
            avg_array_length = 2
            item_size = compute_document_size_from_schema(item_schema)
            total_size += KEY_OVERHEAD + avg_array_length * item_size

        else:
            value_size = TYPE_SIZES.get(field_type, 0)
            total_size += KEY_OVERHEAD + value_size

    return total_size


def compute_collection_size_bytes(collection):
    doc_size = compute_document_size_from_schema(collection.schema)
    return doc_size * collection.document_count


def bytes_to_gb(size_bytes):
    return size_bytes / (1024 ** 3)
