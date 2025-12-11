"""
Query Cost Analysis Script

Computes and displays costs for various query scenarios across DB1-DB5
in terms of: Time, Carbon Footprint, and Price
"""

import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'AppCore'))

from bigdata.query_costs import QueryCostAnalyzer, compare_algorithms


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def print_query_analysis(query_name, documents_scanned, distinct_values, nb_servers):
    """Print analysis for a single query"""
    print(f"\n{query_name}")
    print("-" * 100)
    print(f"  Query pattern: Scan {documents_scanned:,} documents with {distinct_values:,} distinct key values")
    print(f"  Cluster: {nb_servers:,} servers")
    print()
    
    results = compare_algorithms(documents_scanned, distinct_values, nb_servers)
    
    # Header
    print(f"  {'Algorithm':<15} | {'Servers':<10} | {'Time (ms)':<12} | {'Carbon (kg)':<14} | {'Price ($)':<10}")
    print(f"  {'-'*15}-+-{'-'*10}-+-{'-'*12}-+-{'-'*14}-+-{'-'*10}")
    
    # Results for each algorithm
    for algo, data in results.items():
        servers = data['servers_accessed']
        time_ms = data['time_ms']
        carbon = data['carbon_kg']
        price = data['price_usd']
        
        # Quality indicator
        if algo == 'index':
            quality = " [BEST]"
        elif algo == 'shard':
            quality = " [GOOD]"
        elif algo == 'nested_loop':
            quality = " [OK]"
        else:
            quality = " [SLOW]"
        
        print(f"  {algo:<15} | {servers:<10} | {time_ms:<12.4f} | {carbon:<14.6f} | {price:<10.6f}{quality}")


if __name__ == "__main__":
    stats_path = os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json')
    stats = load_json(stats_path)

    nb_products = stats["nb_products"]
    nb_warehouses = stats["nb_warehouses"]
    nb_orderlines = stats["nb_orderlines"]
    nb_clients = stats["nb_clients"]
    nb_servers = stats["nb_servers"]

    print("=" * 100)
    print("QUERY COST ANALYSIS - Section 3")
    print("=" * 100)
    print("\nEstimates for: Time (ms), Carbon Footprint (kg CO2), Price (USD)")
    print("Based on cloud infrastructure parameters and typical query patterns")

    # ===== COMMON QUERIES =====
    print("\n\n" + "=" * 100)
    print("1. PRODUCT QUERIES")
    print("=" * 100)

    print_query_analysis(
        "Find Product by ID",
        documents_scanned=1,
        distinct_values=nb_products,
        nb_servers=nb_servers
    )

    print_query_analysis(
        "Find All Products by Brand (e.g., Apple)",
        documents_scanned=nb_products // 100,  # Assume 1% of products per brand
        distinct_values=5000,  # distinct brands
        nb_servers=nb_servers
    )

    print_query_analysis(
        "Find All Products (full table scan)",
        documents_scanned=nb_products,
        distinct_values=nb_products,
        nb_servers=nb_servers
    )

    # ===== STOCK QUERIES =====
    print("\n\n" + "=" * 100)
    print("2. STOCK QUERIES")
    print("=" * 100)

    nb_stocks = nb_products * nb_warehouses

    print_query_analysis(
        "Find Stock for Product in all Warehouses",
        documents_scanned=nb_warehouses,
        distinct_values=nb_warehouses,
        nb_servers=nb_servers
    )

    print_query_analysis(
        "Find All Stock in Warehouse (e.g., Paris)",
        documents_scanned=nb_products,  # All products in one warehouse
        distinct_values=nb_products,
        nb_servers=nb_servers
    )

    print_query_analysis(
        "Find All Stocks (full table scan)",
        documents_scanned=nb_stocks,
        distinct_values=nb_stocks,
        nb_servers=nb_servers
    )

    # ===== ORDERLINE QUERIES =====
    print("\n\n" + "=" * 100)
    print("3. ORDERLINE QUERIES")
    print("=" * 100)

    print_query_analysis(
        "Find Orders by Client ID",
        documents_scanned=nb_orderlines // nb_clients,  # Orders per client
        distinct_values=nb_clients,
        nb_servers=nb_servers
    )

    print_query_analysis(
        "Find Orders for Product (e.g., iPhone)",
        documents_scanned=nb_orderlines // nb_products,  # Orders per product
        distinct_values=nb_products,
        nb_servers=nb_servers
    )

    print_query_analysis(
        "Find All OrderLines (full table scan)",
        documents_scanned=nb_orderlines,
        distinct_values=nb_orderlines,
        nb_servers=nb_servers
    )

    # ===== SUMMARY =====
    print("\n\n" + "=" * 100)
    print("SUMMARY & RECOMMENDATIONS")
    print("=" * 100)
    print("""
Algorithm Selection Guide:
  [BEST]  Index:      Use when you have indexed field (direct lookup)
                      Example: Find by ID, Find by indexed timestamp
  
  [GOOD]  Shard:      Use with sharding key (balanced distribution)
                      Example: Find by Client ID, Find by Product ID
  
  [OK]    Nested Loop: Use for non-indexed searches
                      Example: Complex filters, range queries
  
  [SLOW]  Full Scan:  Avoid when possible (scans entire cluster)
                      Example: Unindexed searches, complex conditions

Key Observations:
  - Index queries are 1000x+ faster than full scans
  - Sharding strategy impacts cost dramatically
  - Carbon footprint scales with servers accessed + duration
  - Price is proportional to servers accessed + time

Optimization Tips:
  1. Always add indexes for frequently searched fields
  2. Use sharding key for filtering when possible
  3. Avoid full table scans (use pagination instead)
  4. Consider denormalization if read patterns are clear (like DB5)
  5. Monitor query patterns to optimize schema design
""")

    print("=" * 100)
