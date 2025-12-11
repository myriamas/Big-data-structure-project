"""Calcule la taille TOTALE de chaque DB1-DB5 avec toutes les collections"""
import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'AppCore'))

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
    stats_path = os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json')
    schema_dir = os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'schema')
    
    stats = load_json(stats_path)

    nb_products = stats["nb_products"]
    nb_warehouses = stats["nb_warehouses"]
    nb_stocks = nb_products * nb_warehouses
    nb_orderlines = stats["nb_orderlines"]
    nb_clients = stats["nb_clients"]

    # ========== DB1: Prod{[Cat],Supp}, St, Wa, OL, Cl ==========
    print("=" * 70)
    print("DB1: Product (normalized) | Stock | Warehouse | OrderLine | Client")
    print("=" * 70)
    
    db1_prod_doc, _, db1_prod_gb = compute_collection_size_gb(os.path.join(schema_dir, "db1_product_schema.json"), nb_products)
    db1_st_doc, _, db1_st_gb = compute_collection_size_gb(os.path.join(schema_dir, "db3_stock_schema.json"), nb_stocks)
    db1_wa_doc, _, db1_wa_gb = compute_collection_size_gb(os.path.join(schema_dir, "warehouse_schema.json"), nb_warehouses)
    db1_ol_doc, _, db1_ol_gb = compute_collection_size_gb(os.path.join(schema_dir, "db4_orderline_schema.json"), nb_orderlines)
    db1_cl_doc, _, db1_cl_gb = compute_collection_size_gb(os.path.join(schema_dir, "client_schema.json"), nb_clients)
    
    print(f"Product collection:   {db1_prod_gb:.4f} GB")
    print(f"Stock collection:     {db1_st_gb:.4f} GB")
    print(f"Warehouse collection: {db1_wa_gb:.4f} GB")
    print(f"OrderLine collection: {db1_ol_gb:.4f} GB")
    print(f"Client collection:    {db1_cl_gb:.4f} GB")
    db1_total = db1_prod_gb + db1_st_gb + db1_wa_gb + db1_ol_gb + db1_cl_gb
    print(f"\nDB1 TOTAL: {db1_total:.4f} GB")
    print()

    # ========== DB2: Prod{[Cat],Supp, [St]}, Wa, OL, Cl ==========
    print("=" * 70)
    print("DB2: Product (with stocks) | Warehouse | OrderLine | Client")
    print("=" * 70)
    
    db2_prod_doc, _, db2_prod_gb = compute_collection_size_gb(os.path.join(schema_dir, "db2_product_schema.json"), nb_products)
    db2_wa_doc, _, db2_wa_gb = compute_collection_size_gb(os.path.join(schema_dir, "warehouse_schema.json"), nb_warehouses)
    db2_ol_doc, _, db2_ol_gb = compute_collection_size_gb(os.path.join(schema_dir, "db4_orderline_schema.json"), nb_orderlines)
    db2_cl_doc, _, db2_cl_gb = compute_collection_size_gb(os.path.join(schema_dir, "client_schema.json"), nb_clients)
    
    print(f"Product collection:   {db2_prod_gb:.4f} GB")
    print(f"Warehouse collection: {db2_wa_gb:.4f} GB")
    print(f"OrderLine collection: {db2_ol_gb:.4f} GB")
    print(f"Client collection:    {db2_cl_gb:.4f} GB")
    db2_total = db2_prod_gb + db2_wa_gb + db2_ol_gb + db2_cl_gb
    print(f"\nDB2 TOTAL: {db2_total:.4f} GB")
    print()

    # ========== DB3: St{Prod{[Cat],Supp}}, Wa, OL, Cl ==========
    print("=" * 70)
    print("DB3: Stock (with product) | Warehouse | OrderLine | Client")
    print("=" * 70)
    
    db3_st_doc, _, db3_st_gb = compute_collection_size_gb(os.path.join(schema_dir, "db3_stock_schema.json"), nb_stocks)
    db3_wa_doc, _, db3_wa_gb = compute_collection_size_gb(os.path.join(schema_dir, "warehouse_schema.json"), nb_warehouses)
    db3_ol_doc, _, db3_ol_gb = compute_collection_size_gb(os.path.join(schema_dir, "db4_orderline_schema.json"), nb_orderlines)
    db3_cl_doc, _, db3_cl_gb = compute_collection_size_gb(os.path.join(schema_dir, "client_schema.json"), nb_clients)
    
    print(f"Stock collection:     {db3_st_gb:.4f} GB")
    print(f"Warehouse collection: {db3_wa_gb:.4f} GB")
    print(f"OrderLine collection: {db3_ol_gb:.4f} GB")
    print(f"Client collection:    {db3_cl_gb:.4f} GB")
    db3_total = db3_st_gb + db3_wa_gb + db3_ol_gb + db3_cl_gb
    print(f"\nDB3 TOTAL: {db3_total:.4f} GB")
    print()

    # ========== DB4: St, Wa, OL{Prod{[Cat],Supp}}, Cl ==========
    print("=" * 70)
    print("DB4: Stock | Warehouse | OrderLine (with product) | Client")
    print("=" * 70)
    
    db4_st_doc, _, db4_st_gb = compute_collection_size_gb(os.path.join(schema_dir, "db3_stock_schema.json"), nb_stocks)
    db4_wa_doc, _, db4_wa_gb = compute_collection_size_gb(os.path.join(schema_dir, "warehouse_schema.json"), nb_warehouses)
    db4_ol_doc, _, db4_ol_gb = compute_collection_size_gb(os.path.join(schema_dir, "db4_orderline_schema.json"), nb_orderlines)
    db4_cl_doc, _, db4_cl_gb = compute_collection_size_gb(os.path.join(schema_dir, "client_schema.json"), nb_clients)
    
    print(f"Stock collection:     {db4_st_gb:.4f} GB")
    print(f"Warehouse collection: {db4_wa_gb:.4f} GB")
    print(f"OrderLine collection: {db4_ol_gb:.4f} GB")
    print(f"Client collection:    {db4_cl_gb:.4f} GB")
    db4_total = db4_st_gb + db4_wa_gb + db4_ol_gb + db4_cl_gb
    print(f"\nDB4 TOTAL: {db4_total:.4f} GB")
    print()

    # ========== DB5: Prod{[Cat],Supp, [OL]}, St, Wa, Cl ==========
    print("=" * 70)
    print("DB5: Product (with orderlines) | Stock | Warehouse | Client")
    print("=" * 70)
    
    db5_prod_doc, _, db5_prod_gb = compute_collection_size_gb(os.path.join(schema_dir, "db5_product_schema.json"), nb_products)
    db5_st_doc, _, db5_st_gb = compute_collection_size_gb(os.path.join(schema_dir, "db3_stock_schema.json"), nb_stocks)
    db5_wa_doc, _, db5_wa_gb = compute_collection_size_gb(os.path.join(schema_dir, "warehouse_schema.json"), nb_warehouses)
    db5_cl_doc, _, db5_cl_gb = compute_collection_size_gb(os.path.join(schema_dir, "client_schema.json"), nb_clients)
    
    print(f"Product collection:   {db5_prod_gb:.4f} GB")
    print(f"Stock collection:     {db5_st_gb:.4f} GB")
    print(f"Warehouse collection: {db5_wa_gb:.4f} GB")
    print(f"Client collection:    {db5_cl_gb:.4f} GB")
    db5_total = db5_prod_gb + db5_st_gb + db5_wa_gb + db5_cl_gb
    print(f"\nDB5 TOTAL: {db5_total:.4f} GB")
    print()

    # ========== SUMMARY ==========
    print("=" * 70)
    print("SUMMARY - Total Storage Cost per Database")
    print("=" * 70)
    print(f"DB1: {db1_total:.4f} GB")
    print(f"DB2: {db2_total:.4f} GB")
    print(f"DB3: {db3_total:.4f} GB")
    print(f"DB4: {db4_total:.4f} GB")
    print(f"DB5: {db5_total:.4f} GB")
    print()
    totals = [("DB1", db1_total), ("DB2", db2_total), ("DB3", db3_total), ("DB4", db4_total), ("DB5", db5_total)]
    most_efficient = min(totals, key=lambda x: x[1])
    least_efficient = max(totals, key=lambda x: x[1])
    
    print(f"Most efficient:  {most_efficient[0]} ({most_efficient[1]:.4f} GB)")
    print(f"Least efficient: {least_efficient[0]} ({least_efficient[1]:.4f} GB)")
    print(f"Cost multiplier: {least_efficient[1] / most_efficient[1]:.2f}x")
