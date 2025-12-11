"""Simple runner to test operators on repository schemas.

Usage: run from the `BDS_Project\BDS_Project\Scripts` folder with Python.

It loads schemas from `AppCore/schema`, builds `Collection` objects and
executes the four operators for sample filter & join queries.
"""
import json
import os
import sys
from pprint import pprint

# Ensure project root is on path so `AppCore` package imports work
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from AppCore.bigdata.models import Collection
from AppCore.bigdata.operators import (
    filter_with_sharding,
    filter_without_sharding,
    nested_loop_with_sharding,
    nested_loop_without_sharding,
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

    # Load product and stock schemas (use product_schema and db3_stock as example)
    product_schema = load_schema(os.path.join(base, 'product_schema.json'))
    stock_schema_db3 = load_schema(os.path.join(base, 'db3_stock_schema.json'))

    # Build Collection objects
    product_collection = Collection('product', product_schema, stats, document_count=stats.get('nb_products', 100000))
    stock_collection_db3 = Collection('stock_db3', stock_schema_db3, stats, document_count=stats.get('nb_products', 100000) * stats.get('nb_warehouses', 200))

    print('\n-- Filter operators (brand) --')
    res1 = filter_with_sharding(product_collection, expected_output_keys=['name', 'price'], filtered_key='brand')
    res2 = filter_without_sharding(product_collection, expected_output_keys=['name', 'price'], filtered_key='brand')

    pprint(res1)
    pprint(res2)

    print('\n-- Nested-loop join operators (product × stock) --')
    # Test nested-loop between product and stock (DB3 has embedded product)
    res3 = nested_loop_with_sharding(product_collection, stock_collection_db3, expected_output_keys=['name','price','idw','quantity'], filtered_key='brand')
    res4 = nested_loop_without_sharding(product_collection, stock_collection_db3, expected_output_keys=['name','price','idw','quantity'], filtered_key='brand')

    pprint(res3)
    pprint(res4)


if __name__ == '__main__':
    main()
