"""évalue répartition sur 1k serveurs"""
import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'AppCore'))

from bigdata.sharding import compute_sharding_distribution


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def show_case(label, document_count, distinct_key_values, nb_servers):
    result = compute_sharding_distribution(document_count, distinct_key_values, nb_servers)
    print(f"{label}")
    print(f"  Average docs per server:         {result['avg_docs_per_server']:.2f}")
    print(f"  Average distinct key values:     {result['avg_distinct_values_per_server']:.2f}")
    print()


if __name__ == "__main__":
    stats_path = os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json')
    stats = load_json(stats_path)

    nb_products = stats["nb_products"]
    nb_warehouses = stats["nb_warehouses"]
    nb_orderlines = stats["nb_orderlines"]
    nb_clients = stats["nb_clients"]
    nb_servers = stats["nb_servers"]
    distinct_brands = stats["distinct_brands"]

    # Stock collection size = one entry per (product, warehouse)
    nb_stocks = nb_products * nb_warehouses

    print("=" * 80)
    print("SHARDING STRATEGIES ANALYSIS - 2.6.1")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  Total Servers:      {nb_servers}")
    print(f"  Nb Products:        {nb_products}")
    print(f"  Nb Warehouses:      {nb_warehouses}")
    print(f"  Nb Stocks:          {nb_stocks}")
    print(f"  Nb OrderLines:      {nb_orderlines}")
    print(f"  Nb Clients:         {nb_clients}")
    print(f"  Distinct Brands:    {distinct_brands}")
    print("\n" + "=" * 80)
    print("SHARDING RESULTS")
    print("=" * 80 + "\n")

    # 1) St - #IDP
    show_case(
        "St - #IDP",
        document_count=nb_stocks,
        distinct_key_values=nb_products,
        nb_servers=nb_servers
    )

    # 2) St - #IDW
    show_case(
        "St - #IDW",
        document_count=nb_stocks,
        distinct_key_values=nb_warehouses,
        nb_servers=nb_servers
    )

    # 3) OL - #IDC
    show_case(
        "OL - #IDC",
        document_count=nb_orderlines,
        distinct_key_values=nb_clients,
        nb_servers=nb_servers
    )

    # 4) OL - #IDP
    show_case(
        "OL - #IDP",
        document_count=nb_orderlines,
        distinct_key_values=nb_products,
        nb_servers=nb_servers
    )

    # 5) Prod - #IDP
    show_case(
        "Prod - #IDP",
        document_count=nb_products,
        distinct_key_values=nb_products,
        nb_servers=nb_servers
    )

    # 6) Prod - #brand
    show_case(
        "Prod - #brand",
        document_count=nb_products,
        distinct_key_values=distinct_brands,
        nb_servers=nb_servers
    )

    print("=" * 80)
    print("INTERPRETATION")
    print("=" * 80)
    print("""
Good sharding strategies have:
  ✓ High avg distinct key values (good distribution across servers)
  ✓ Low variance (balanced load)
  
Problems:
  ✗ Too few distinct values → some servers will be overloaded
  ✗ Uneven distribution → data hotspots
""")
