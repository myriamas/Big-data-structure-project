#!/usr/bin/env python3
"""
Integration Demo Script - Part 5

Demonstrates execution of complex aggregate queries (Q6, Q7) across
different database schemas (DB1, DB4, DB5) and compares their costs.

Usage:
    python Scripts/run_integration.py [--query Q6|Q7|all] [--format table|json]
"""
import sys
import os
import json
import argparse

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from AppCore.bigdata import IntegrationExecutor


def load_json(path):
    """Load a JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description='Run Q6/Q7 integration queries')
    parser.add_argument('--query', choices=['Q6', 'Q7', 'all'], default='all',
                        help='Query to execute (default: all)')
    parser.add_argument('--format', choices=['table', 'json'], default='table',
                        help='Output format (default: table)')
    args = parser.parse_args()

    # Get paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    schema_dir = os.path.join(base_dir, 'AppCore', 'schema')
    stats_path = os.path.join(base_dir, 'AppCore', 'stats.json')

    # Load statistics
    stats = load_json(stats_path)
    print(f"Loaded statistics: {stats['nb_orderlines']:,} orderlines, {stats['nb_products']:,} products")
    print()

    # Load schemas for different DB configurations
    schemas = {}

    # DB1: Normalized (product only, separate orderline)
    db1_path = os.path.join(schema_dir, 'db1_product_schema.json')
    if os.path.exists(db1_path):
        schemas['DB1'] = load_json(db1_path)

    # DB4: OrderLine with embedded Product
    db4_path = os.path.join(schema_dir, 'db4_orderline_schema.json')
    if os.path.exists(db4_path):
        schemas['DB4'] = load_json(db4_path)

    # DB5: Product with embedded OrderLines
    db5_path = os.path.join(schema_dir, 'db5_product_schema.json')
    if os.path.exists(db5_path):
        schemas['DB5'] = load_json(db5_path)

    if not schemas:
        print("ERROR: No schemas found in", schema_dir)
        sys.exit(1)

    print(f"Loaded schemas: {list(schemas.keys())}")
    print()

    # Create executor
    executor = IntegrationExecutor(schemas, stats)

    # Execute queries
    queries_to_run = ['Q6', 'Q7'] if args.query == 'all' else [args.query]

    for query_id in queries_to_run:
        print("=" * 70)
        print(f"QUERY {query_id}")
        print("=" * 70)

        if query_id == 'Q6':
            print("Top 100 most ordered products (SUM of quantities)")
            print("SQL: SELECT P.name, P.price, SUM(O.quantity) FROM OrderLine O")
            print("     JOIN Product P ON O.IDP = P.IDP GROUP BY O.IDP")
            print("     ORDER BY SUM(O.quantity) DESC LIMIT 100")
        elif query_id == 'Q7':
            print("Most ordered product by customer #125")
            print("SQL: SELECT P.name, P.price, SUM(O.quantity) FROM OrderLine O")
            print("     JOIN Product P ON O.IDP = P.IDP WHERE O.IDC = 125")
            print("     GROUP BY O.IDP ORDER BY SUM(O.quantity) DESC LIMIT 1")
        print()

        if args.format == 'table':
            # Print comparison table
            table = executor.print_comparison_table(query_id, list(schemas.keys()))
            print(table)
        else:
            # Print JSON
            comparison = executor.compare_query_across_dbs(query_id, list(schemas.keys()))
            print(json.dumps(comparison, indent=2, default=str))

        print()

    # Summary
    print("=" * 70)
    print("SUMMARY - Best Database Recommendations")
    print("=" * 70)
    for query_id in queries_to_run:
        best_db = executor.get_best_db_for_query(query_id)
        print(f"  {query_id}: Use {best_db}")
    print()


if __name__ == '__main__':
    main()
