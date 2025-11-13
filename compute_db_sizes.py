import json
from bigdata.sizes import compute_document_size_from_schema, bytes_to_gb


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def compute_collection_size_gb(schema_path, document_count):
    schema = load_json(schema_path)
    doc_size = compute_document_size_from_schema(schema)
    total_size = doc_size * document_count
    return doc_size, total_size, bytes_to_gb(total_size)


if __name__ == "__main__":
    stats = load_json("stats.json")

    nb_products = stats["nb_products"]
    nb_stocks = stats["nb_products"] * stats["nb_warehouses"]
    nb_orderlines = stats["nb_orderlines"]

    print("### DB1 ###")
    product_doc, product_total, product_gb = compute_collection_size_gb("schema/db1_product_schema.json", nb_products)
    print("Product document size:", product_doc, "bytes")
    print("Product collection size:", product_gb, "GB")
    print()

    print("### DB2 ###")
    product_doc, product_total, product_gb = compute_collection_size_gb("schema/db2_product_schema.json", nb_products)
    print("Product document size:", product_doc, "bytes")
    print("Product collection size:", product_gb, "GB")
    print()

    print("### DB3 ###")
    stock_doc, stock_total, stock_gb = compute_collection_size_gb("schema/db3_stock_schema.json", nb_stocks)
    print("Stock document size:", stock_doc, "bytes")
    print("Stock collection size:", stock_gb, "GB")
    print()

    print("### DB4 ###")
    ol_doc, ol_total, ol_gb = compute_collection_size_gb("schema/db4_orderline_schema.json", nb_orderlines)
    print("OrderLine document size:", ol_doc, "bytes")
    print("OrderLine collection size:", ol_gb, "GB")
    print()

    print("### DB5 ###")
    product_doc, product_total, product_gb = compute_collection_size_gb("schema/db5_product_schema.json", nb_products)  
    print("Product document size:", product_doc, "bytes")
    print("Product collection size:", product_gb, "GB")
    print()
