import json
from bigdata.sharding import compute_sharding_distribution


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def show_case(label, document_count, distinct_key_values, nb_servers):
    result = compute_sharding_distribution(document_count, distinct_key_values, nb_servers)
    print(label)
    print("  avg documents per server:", result["avg_docs_per_server"])
    print("  avg distinct key values per server:", result["avg_distinct_values_per_server"])
    print()


if __name__ == "__main__":
    stats = load_json("stats.json")

    nb_products = stats["nb_products"]
    nb_warehouses = stats["nb_warehouses"]
    nb_orderlines = stats["nb_orderlines"]
    nb_clients = stats["nb_clients"]
    nb_servers = stats["nb_servers"]
    distinct_brands = stats["distinct_brands"]

    # Stock collection size = one entry per (product, warehouse)
    nb_stocks = nb_products * nb_warehouses

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
