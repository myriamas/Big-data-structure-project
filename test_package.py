"""
Test script for the bigdata package
Validates all main functions and classes
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'AppCore'))

from bigdata import (
    Collection,
    compute_document_size_from_schema,
    compute_collection_size_bytes,
    compute_database_size_gb,
    bytes_to_gb,
    compute_sharding_distribution
)
import json


def load_json(path):
    with open(path) as f:
        return json.load(f)


def test_package():
    """Run comprehensive tests on the bigdata package"""
    
    print("=" * 70)
    print("BDS PACKAGE TEST SUITE - 2.7 Homework")
    print("=" * 70 + "\n")
    
    # Load test data
    schema_path = "AppCore/schema/db1_product_schema.json"
    stats_path = "AppCore/stats.json"
    
    schema = load_json(schema_path)
    stats = load_json(stats_path)
    
    # TEST 1: Collection Creation
    print("TEST 1: Collection Creation and Validation")
    print("-" * 70)
    try:
        product = Collection("Product", schema, stats, 100000)
        print(f"✓ Created collection: {product}")
        assert product.name == "Product"
        assert product.document_count == 100000
        print("✓ Attributes validated")
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False
    print()
    
    # TEST 2: Size Computation - Document
    print("TEST 2: Document Size Computation")
    print("-" * 70)
    try:
        doc_size = compute_document_size_from_schema(schema)
        print(f"✓ Document size: {doc_size} bytes")
        assert doc_size > 0
        print("✓ Size is positive and valid")
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False
    print()
    
    # TEST 3: Collection Size Computation
    print("TEST 3: Collection Size Computation")
    print("-" * 70)
    try:
        coll_bytes = compute_collection_size_bytes(product)
        coll_gb = bytes_to_gb(coll_bytes)
        print(f"✓ Collection size: {coll_bytes} bytes")
        print(f"✓ Collection size: {coll_gb:.4f} GB")
        assert coll_bytes == doc_size * 100000
        print("✓ Calculation verified (doc_size * document_count)")
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False
    print()
    
    # TEST 4: Database Size Computation
    print("TEST 4: Database Size Computation")
    print("-" * 70)
    try:
        stock_schema = load_json("AppCore/schema/db3_stock_schema.json")
        ol_schema = load_json("AppCore/schema/db4_orderline_schema.json")
        
        collections = [
            Collection("Product", schema, stats, 100000),
            Collection("Stock", stock_schema, stats, 20_000_000),
            Collection("OrderLine", ol_schema, stats, 4_000_000_000)
        ]
        
        db_size_gb = compute_database_size_gb(collections)
        print(f"✓ Database total size: {db_size_gb:.2f} GB")
        print(f"  - Product: {bytes_to_gb(compute_collection_size_bytes(collections[0])):.4f} GB")
        print(f"  - Stock: {bytes_to_gb(compute_collection_size_bytes(collections[1])):.4f} GB")
        print(f"  - OrderLine: {bytes_to_gb(compute_collection_size_bytes(collections[2])):.4f} GB")
        assert db_size_gb > 0
        print("✓ Database size calculation validated")
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False
    print()
    
    # TEST 5: Sharding Distribution
    print("TEST 5: Sharding Distribution Analysis")
    print("-" * 70)
    try:
        strategies = [
            ("St - #IDP", 20_000_000, 100_000),
            ("St - #IDW", 20_000_000, 200),
            ("Prod - #IDP", 100_000, 100_000),
            ("Prod - #brand", 100_000, 5_000),
        ]
        
        for name, docs, distinct in strategies:
            result = compute_sharding_distribution(docs, distinct, 1000)
            avg_docs = result['avg_docs_per_server']
            avg_keys = result['avg_distinct_values_per_server']
            quality = "✓ Good" if avg_keys >= 1 else "✗ Bad (hotspots)"
            print(f"  {name:20} | docs={avg_docs:10.0f} | keys={avg_keys:8.2f} {quality}")
        
        print("✓ All sharding strategies evaluated")
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False
    print()
    
    # TEST 6: Error Handling
    print("TEST 6: Error Handling and Validation")
    print("-" * 70)
    try:
        # Test TypeError for invalid schema
        try:
            Collection("Bad", "not a dict", {}, 100)
            print("✗ Should have raised TypeError for invalid schema")
            return False
        except TypeError:
            print("✓ TypeError correctly raised for invalid schema")
        
        # Test ValueError for negative document count
        try:
            Collection("Bad", {}, {}, -100)
            print("✗ Should have raised ValueError for negative count")
            return False
        except ValueError:
            print("✓ ValueError correctly raised for negative document count")
        
        print("✓ Error handling validated")
    except Exception as e:
        print(f"✗ FAILED: {e}")
        return False
    print()
    
    print("=" * 70)
    print("✓ ALL TESTS PASSED - Package is fully functional!")
    print("=" * 70)
    return True


if __name__ == "__main__":
    success = test_package()
    sys.exit(0 if success else 1)
