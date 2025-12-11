"""Operators runner with CLI support and formatted output.

Usage:
    python run_operators.py                    # Run all operators with defaults
    python run_operators.py --format table     # Print as ASCII table
    python run_operators.py --format json      # Output as JSON
    python run_operators.py --selectivity 0.1  # Override selectivity
    python run_operators.py --help             # Show all options

Loads schemas from `AppCore/schema`, builds `Collection` objects and
executes the four operators for filter & join queries with configurable output.
"""
import argparse
import json
import os
import sys
import csv
from io import StringIO
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
    """Load JSON schema from file."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def format_as_table(results):
    """Format results as an ASCII table."""
    lines = []
    lines.append("OPERATOR RESULTS")
    lines.append("=" * 120)
    
    # Filter results
    if 'filters' in results:
        lines.append("\nFILTER OPERATORS (product collection, brand filter)")
        lines.append("-" * 120)
        lines.append(f"{'Operator':<30} {'Output Docs':>12} {'Output (MB)':>12} {'Time (ms)':>12} {'Cost (USD)':>12} {'Carbon (kg)':>12}")
        lines.append("-" * 120)
        for res in results['filters']:
            out_mb = res['output_bytes'] / (1024 * 1024)
            lines.append(
                f"{res['operator']:<30} {res['output_docs']:>12} {out_mb:>12.3f} "
                f"{res['cost']['time_ms']:>12.3f} {res['cost']['price_usd']:>12.6f} "
                f"{res['cost']['carbon_kg']:>12.9f}"
            )
    
    # Nested-loop results
    if 'joins' in results:
        lines.append("\nNESTED-LOOP JOIN OPERATORS (product x stock)")
        lines.append("-" * 120)
        lines.append(f"{'Operator':<40} {'Docs Scanned':>15} {'Output Docs':>15} {'Time (ms)':>15} {'Cost (USD)':>12}")
        lines.append("-" * 120)
        for res in results['joins']:
            lines.append(
                f"{res['operator']:<40} {res.get('docs_scanned', '-'):>15} {res['output_docs']:>15} "
                f"{res['cost']['time_ms']:>15.2f} {res['cost']['price_usd']:>12.6f}"
            )
    
    lines.append("=" * 120)
    return "\n".join(lines)


def format_as_json(results):
    """Format results as pretty JSON."""
    return json.dumps(results, indent=2)


def run_operators(selectivity=None):
    """Run all four operators and return results."""
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'schema'))
    stats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json'))

    stats = {}
    if os.path.exists(stats_path):
        with open(stats_path, 'r', encoding='utf-8') as f:
            stats = json.load(f)

    # Load product and stock schemas
    product_schema = load_schema(os.path.join(base, 'product_schema.json'))
    stock_schema_db3 = load_schema(os.path.join(base, 'db3_stock_schema.json'))

    # Build Collection objects
    product_collection = Collection(
        'product',
        product_schema,
        stats,
        document_count=stats.get('nb_products', 100000)
    )
    stock_collection_db3 = Collection(
        'stock_db3',
        stock_schema_db3,
        stats,
        document_count=stats.get('nb_products', 100000) * stats.get('nb_warehouses', 200)
    )

    results = {
        'filters': [],
        'joins': []
    }

    # Run filter operators
    res1 = filter_with_sharding(
        product_collection,
        expected_output_keys=['name', 'price'],
        filtered_key='brand',
        selectivity=selectivity
    )
    res2 = filter_without_sharding(
        product_collection,
        expected_output_keys=['name', 'price'],
        filtered_key='brand',
        selectivity=selectivity
    )
    results['filters'].append(res1)
    results['filters'].append(res2)

    # Run nested-loop operators
    res3 = nested_loop_with_sharding(
        product_collection,
        stock_collection_db3,
        expected_output_keys=['name', 'price', 'idw', 'quantity'],
        filtered_key='brand',
        selectivity=selectivity
    )
    res4 = nested_loop_without_sharding(
        product_collection,
        stock_collection_db3,
        expected_output_keys=['name', 'price', 'idw', 'quantity'],
        filtered_key='brand',
        selectivity=selectivity
    )
    results['joins'].append(res3)
    results['joins'].append(res4)

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Run DVL operators with configurable output and selectivity.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_operators.py
    python run_operators.py --format table
    python run_operators.py --format json --selectivity 0.1
        """
    )
    parser.add_argument('--format', choices=['pprint', 'table', 'json'], default='table',
                        help='Output format (default: table)')
    parser.add_argument('--selectivity', type=float, default=None,
                        help='Override selectivity (0.0-1.0, default: auto-estimate)')
    
    args = parser.parse_args()
    
    # Validate selectivity
    if args.selectivity is not None and not (0.0 <= args.selectivity <= 1.0):
        parser.error("selectivity must be between 0.0 and 1.0")
    
    results = run_operators(selectivity=args.selectivity)
    
    if args.format == 'table':
        print(format_as_table(results))
    elif args.format == 'json':
        print(format_as_json(results))
    else:
        pprint(results)


if __name__ == '__main__':
    main()
