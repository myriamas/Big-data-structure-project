import json
from bigdata.models import Collection
from bigdata.sizes import compute_document_size_from_schema, compute_collection_size_bytes, bytes_to_gb


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    product_schema = load_json("schema/teacher_product_schema.json")
    stats = load_json("stats.json")

    nb_products = stats["nb_products"]

    product_collection = Collection(
        name="Product",
        schema=product_schema,
        stats=stats,
        document_count=nb_products
    )

    doc_size = compute_document_size_from_schema(product_collection.schema)
    coll_size_bytes = compute_collection_size_bytes(product_collection)
    coll_size_gb = bytes_to_gb(coll_size_bytes)

    print("Product document size (bytes):", doc_size)
    print("Product collection size (bytes):", coll_size_bytes)
    print("Product collection size (GB):", coll_size_gb)
