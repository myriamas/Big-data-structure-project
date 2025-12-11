"""
Query analysis module for NoSQL database operations.
Provides query simulation and cost estimation for different DB schemas.
"""

from .sizes import compute_document_size_from_schema
from .sharding import compute_sharding_distribution
from typing import Dict, List, Any

# Global constants
NB_SERVERS = 1000
DOCS_PER_SERVER = 4_000_000
POWER_PER_SERVER_WATTS = 400
POWER_NETWORK_WATTS = 50
CLOUD_COST_PER_SERVER_HOUR = 0.50
CO2_PER_KWH = 0.233


class QueryExecutor:
    """Simulates and analyzes NoSQL query execution costs."""
    
    def __init__(self, databases: Dict[str, Dict[str, Any]], stats: Dict[str, int]):
        """
        Initialize query executor.
        
        Args:
            databases: Dict of database schemas {db_name: {collection_name: schema}}
            stats: Statistics {num_products, num_clients, num_orderlines, num_warehouses}
        """
        self.databases = databases
        self.stats = stats
        self.results = {}
    
    def execute_q1_simple_stock_lookup(self, db_name: str) -> Dict[str, Any]:
        """
        Q1: Stock of a given product in a given warehouse
        SELECT S.quantity, S.location
        FROM Stock S
        WHERE S.IDP = $IDP AND S.IDW = $IDW;
        
        MongoDB: db.stock.findOne({"idp": 1234, "idw": 5})
        """
        stock_schema = self.databases[db_name].get('stock', {})
        doc_size = compute_document_size_from_schema(stock_schema) if stock_schema else 500
        
        # Composite index lookup: very fast
        docs_scanned = 1
        servers_accessed = 1
        time_ms = 0.001  # Index lookup is nearly instant
        
        return self._compute_costs("Q1", db_name, docs_scanned, servers_accessed, time_ms, doc_size)
    
    def execute_q2_filter_by_brand(self, db_name: str, brand: str = "Apple") -> Dict[str, Any]:
        """
        Q2: Names and prices of products from a brand
        SELECT P.name, P.price
        FROM Product P
        WHERE P.brand = $brand;
        
        MongoDB: db.product.find({"brand": "Apple"})
        """
        # Actual execution logic below
        product_schema = self.databases[db_name].get('product', {})
        doc_size = compute_document_size_from_schema(product_schema) if product_schema else 1000
        
        num_products = self.stats.get('num_products', 100_000)
        num_brands = self.stats.get('num_brands', 5_000)
        
        # Average products per brand
        products_per_brand = num_products // num_brands
        # Apple is popular, assume 5x average
        apple_products = products_per_brand * 5
        
        # Index lookup
        docs_scanned = apple_products
        servers_accessed = 1
        time_ms = apple_products * 0.001  # 1 microsecond per doc
        
        return self._compute_costs("Q2", db_name, docs_scanned, servers_accessed, time_ms, doc_size)
    
    def execute_q3_filter_by_date(self, db_name: str, days_of_data: int = 365) -> Dict[str, Any]:
        """
        Q3: Product ID and quantity from order lines at a given date
        SELECT O.IDP, O.quantity
        FROM OrderLine O
        WHERE O.date = $date;
        
        MongoDB: db.orderline.find({"date": ISODate("2024-12-11")})
        """
        orderline_schema = self.databases[db_name].get('orderline', {})
        doc_size = compute_document_size_from_schema(orderline_schema) if orderline_schema else 300
        
        num_orderlines = self.stats.get('num_orderlines', 4_000_000_000)
        
        # Average orderlines per day
        orderlines_per_day = num_orderlines // days_of_data
        
        # Index lookup
        docs_scanned = orderlines_per_day
        servers_accessed = max(1, docs_scanned // DOCS_PER_SERVER)
        time_ms = docs_scanned * 0.000001  # 1 nanosecond per doc with index
        
        return self._compute_costs("Q3", db_name, docs_scanned, servers_accessed, time_ms, doc_size)
    
    def execute_q4_join_stock_with_product(self, db_name: str, warehouse_id: int = 5) -> Dict[str, Any]:
        """
        Q4: Stock (list of product names, as well as their quantity) from a given warehouse
        SELECT P.name, S.quantity
        FROM Stock S
        JOIN Product P ON S.IDP = P.IDP
        WHERE S.IDW = $IDW;
        
        MongoDB (DB1): db.stock.aggregate([{$match}, {$lookup}, {$project}])
        MongoDB (DB2/DB3): db.stock.find({...}) - Product embedded
        """
        num_warehouses = self.stats.get('num_warehouses', 200)
        num_products = self.stats.get('num_products', 100_000)
        
        # Products per warehouse (roughly uniform)
        products_per_warehouse = num_products
        
        stock_schema = self.databases[db_name].get('stock', {})
        doc_size = compute_document_size_from_schema(stock_schema) if stock_schema else 1000
        
        # Check if Product is embedded in Stock (DB3 specifically)
        has_embedded_product = 'product' in stock_schema.get('properties', {}) if stock_schema else False
        
        docs_scanned = products_per_warehouse
        servers_accessed = 2  # Default: need JOIN
        
        if has_embedded_product:
            # DB3: Embedded product, very fast (no join needed)
            time_ms = products_per_warehouse * 0.000005  # 5 microseconds per doc
            servers_accessed = 1  # Only one server needed
        else:
            # DB1, DB4, DB5: Need JOIN/aggregation, slower
            time_ms = products_per_warehouse * 0.001  # 1 millisecond per doc (with join overhead)
            servers_accessed = 2  # Two collections accessed
        
        return self._compute_costs("Q4", db_name, docs_scanned, servers_accessed, time_ms, doc_size, 
                                  has_embedded_product)
    
    def execute_q5_join_apple_products_in_warehouses(self, db_name: str) -> Dict[str, Any]:
        """
        Q5: Distribution of "Apple" brand products (name & price) in warehouses (IDW & quantity)
        SELECT P.name, P.price, S.IDW, S.quantity
        FROM Product P
        JOIN Stock S ON P.IDP = S.IDP
        WHERE P.brand = "Apple";
        
        MongoDB (DB1, DB2): Product.find({brand: Apple}) → lookup Stock
        MongoDB (DB3): Stock.find({product.brand: Apple}) - Product embedded!
        """
        num_products = self.stats.get('num_products', 100_000)
        num_brands = self.stats.get('num_brands', 5_000)
        num_warehouses = self.stats.get('num_warehouses', 200)
        
        # Apple products: 5x average
        products_per_brand = num_products // num_brands
        apple_products = products_per_brand * 5  # ~100 Apple products
        
        # Each product in each warehouse
        stock_entries = apple_products * num_warehouses
        
        stock_schema = self.databases[db_name].get('stock', {})
        doc_size = compute_document_size_from_schema(stock_schema) if stock_schema else 1000
        
        # Check if Product is embedded in Stock (DB3 only)
        has_product_embedded = 'product' in stock_schema.get('properties', {}) if stock_schema else False
        
        docs_scanned = stock_entries
        servers_accessed = 2  # Default: need JOIN
        
        if has_product_embedded:
            # DB3: Query Stock directly on embedded product.brand, very fast
            time_ms = stock_entries * 0.00001  # 10 microseconds per doc
            servers_accessed = 1  # Only one server needed
        else:
            # DB1, DB2, DB4, DB5: Must join Product → Stock, slower
            time_ms = stock_entries * 0.001  # 1 millisecond per doc
            servers_accessed = 2
        
        return self._compute_costs("Q5", db_name, docs_scanned, servers_accessed, time_ms, doc_size,
                                  has_product_embedded)
    
    def _compute_costs(self, query_id: str, db_name: str, docs_scanned: int, 
                      servers_accessed: int, time_ms: float, doc_size: int,
                      optimization: bool = False) -> Dict[str, Any]:
        """
        Compute time, carbon, and price costs for a query.
        
        Args:
            query_id: Query identifier (Q1-Q5)
            db_name: Database name
            docs_scanned: Number of documents processed
            servers_accessed: Number of servers involved
            time_ms: Execution time in milliseconds
            doc_size: Average document size in bytes
            optimization: Whether query uses optimization (index, embedding)
        
        Returns:
            Dict with cost metrics
        """
        # Convert time to seconds
        time_seconds = time_ms / 1000.0
        
        # Carbon footprint calculation
        # Power = (servers × 400W) + (network overhead 50W)
        power_watts = (servers_accessed * POWER_PER_SERVER_WATTS) + POWER_NETWORK_WATTS
        power_kw = power_watts / 1000.0
        # Energy = Power × Time (in hours)
        time_hours = time_seconds / 3600.0
        energy_kwh = power_kw * time_hours
        carbon_kg = energy_kwh * CO2_PER_KWH
        
        # Cloud cost calculation
        # Cost = servers × cost_per_server × time_hours
        cost_usd = servers_accessed * CLOUD_COST_PER_SERVER_HOUR * time_hours
        
        result = {
            'query_id': query_id,
            'db_name': db_name,
            'docs_scanned': docs_scanned,
            'servers_accessed': servers_accessed,
            'time_ms': round(time_ms, 3),
            'time_seconds': round(time_seconds, 6),
            'carbon_kg': round(carbon_kg, 9),
            'cost_usd': round(cost_usd, 6),
            'optimization': 'Yes' if optimization else 'No',
            'quality': self._assess_quality(time_ms, servers_accessed)
        }
        
        return result
    
    def _assess_quality(self, time_ms: float, servers_accessed: int) -> str:
        """Rate query performance quality."""
        if time_ms < 1 and servers_accessed == 1:
            return "[5/5] EXCELLENT"
        elif time_ms < 10 and servers_accessed <= 2:
            return "[4/5] GOOD"
        elif time_ms < 100 and servers_accessed <= 5:
            return "[3/5] OK"
        else:
            return "[2/5] SLOW"
    
    def execute_all_queries(self) -> Dict[str, List[Dict[str, Any]]]:
        """Execute all 5 queries across all databases."""
        results = {}
        
        for db_name in self.databases.keys():
            results[db_name] = {
                'Q1': self.execute_q1_simple_stock_lookup(db_name),
                'Q2': self.execute_q2_filter_by_brand(db_name),
                'Q3': self.execute_q3_filter_by_date(db_name),
                'Q4': self.execute_q4_join_stock_with_product(db_name),
                'Q5': self.execute_q5_join_apple_products_in_warehouses(db_name),
            }
        
        return results
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate summary of best DB for each query."""
        results = self.execute_all_queries()
        
        # Compare total costs across DBs for each query
        summary = {}
        queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
        
        for query in queries:
            best_db = min(
                self.databases.keys(),
                key=lambda db: results[db][query]['cost_usd']
            )
            best_cost = results[best_db][query]['cost_usd']
            best_time = results[best_db][query]['time_ms']
            
            summary[query] = {
                'best_db': best_db,
                'cost_usd': best_cost,
                'time_ms': best_time,
                'quality': results[best_db][query]['quality']
            }
        
        return summary
