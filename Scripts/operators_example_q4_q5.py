"""Integration example: Using operators for Q4 and Q5 queries.

This demonstrates how to use the operators package to analyze the cost
of real-world queries (Q4 and Q5 from the practice session) with different
database schemas (DB1, DB3, etc.).
"""
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from AppCore.bigdata.models import Collection
from AppCore.bigdata.operators import (
    filter_with_sharding,
    nested_loop_with_sharding,
)


def load_schema(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'schema'))
    stats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json'))

    stats = {}
    if os.path.exists(stats_path):
        with open(stats_path, 'r', encoding='utf-8') as f:
            stats = json.load(f)

    # Load schemas
    product_schema = load_schema(os.path.join(base, 'product_schema.json'))
    stock_db1 = load_schema(os.path.join(base, 'db1_product_schema.json'))
    stock_db3 = load_schema(os.path.join(base, 'db3_stock_schema.json'))

    print("[Q4] Stock by warehouse with product names\n")
    print("Query: SELECT P.name, S.quantity FROM Stock S")
    print("       JOIN Product P ON S.IDP = P.IDP WHERE S.IDW = $IDW\n")

    # DB1: Separate collections, needs join
    db1_stock = Collection('stock_db1', stock_db1, stats, document_count=100000 * 200)
    db1_product = Collection('product_db1', product_schema, stats, document_count=100000)
    
    print("DB1 (separate collections, nested-loop join):")
    result_db1 = nested_loop_with_sharding(
        db1_stock,
        db1_product,
        expected_output_keys=['name', 'quantity'],
        filtered_key='warehouse_id',
        selectivity=0.005  # Filter for warehouse 5 out of 200
    )
    print(f"  Docs scanned: {result_db1['docs_scanned']}")
    print(f"  Output docs: {result_db1['output_docs']}")
    print(f"  Time: {result_db1['cost']['time_ms']:.2f} ms")
    print(f"  Cost: ${result_db1['cost']['price_usd']:.6f}")
    print()

    # DB3: Product embedded, just a filter
    db3_stock = Collection('stock_db3', stock_db3, stats, document_count=100000 * 200)
    
    print("DB3 (embedded product, simple filter):")
    result_db3 = filter_with_sharding(
        db3_stock,
        expected_output_keys=['product.name', 'quantity'],
        filtered_key='warehouse_id',
        selectivity=0.005  # Same warehouse filter
    )
    print(f"  Docs scanned: {result_db3['output_docs']}")
    print(f"  Output docs: {result_db3['output_docs']}")
    print(f"  Time: {result_db3['cost']['time_ms']:.2f} ms")
    print(f"  Cost: ${result_db3['cost']['price_usd']:.6f}")
    print()
    
    print(f"[Conclusion] DB3 is {result_db1['cost']['time_ms'] / result_db3['cost']['time_ms']:.0f}x faster for Q4!")
    print()

    print("\n[Q5] Apple products in all warehouses\n")
    print("Query: SELECT P.name, P.price, S.IDW, S.quantity FROM Product P")
    print("       JOIN Stock S ON P.IDP = S.IDP WHERE P.brand = 'Apple'\n")

    # DB1: Filter product then join with stock
    print("DB1 (filter + nested-loop join):")
    db1_apple_products = Collection('apple_products', product_schema, stats, document_count=50)
    result_db1_q5 = nested_loop_with_sharding(
        db1_apple_products,
        db1_stock,
        expected_output_keys=['name', 'price', 'warehouse_id', 'quantity'],
        filtered_key='brand',
        selectivity=0.0005  # 50 Apple products / 100k products
    )
    print(f"  Docs scanned: {result_db1_q5['docs_scanned']}")
    print(f"  Output docs: {result_db1_q5['output_docs']}")
    print(f"  Time: {result_db1_q5['cost']['time_ms']:.2f} ms")
    print(f"  Cost: ${result_db1_q5['cost']['price_usd']:.6f}")
    print()

    # DB3: Query stock with embedded product filter on brand
    print("DB3 (filter on embedded product.brand):")
    db3_apple_stock = Collection('apple_stock_db3', stock_db3, stats, document_count=50 * 200)
    result_db3_q5 = filter_with_sharding(
        db3_apple_stock,
        expected_output_keys=['product.name', 'product.price', 'warehouse_id', 'quantity'],
        filtered_key='product.brand',
        selectivity=0.0005  # 50 * 200 Apple stock entries / (100k * 200 total)
    )
    print(f"  Docs scanned: {result_db3_q5['output_docs']}")
    print(f"  Output docs: {result_db3_q5['output_docs']}")
    print(f"  Time: {result_db3_q5['cost']['time_ms']:.2f} ms")
    print(f"  Cost: ${result_db3_q5['cost']['price_usd']:.6f}")
    print()

    print(f"[Conclusion] DB3 is {result_db1_q5['cost']['time_ms'] / result_db3_q5['cost']['time_ms']:.0f}x faster for Q5!")


if __name__ == '__main__':
    main()
