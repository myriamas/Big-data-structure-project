"""
Query Cost Analysis Module

Computes estimated costs for NoSQL queries in terms of:
- Time (milliseconds)
- Carbon Footprint (kg CO2)
- Price (USD)

Based on:
- Documents scanned
- Sharding strategy (affects number of servers accessed)
- Query algorithm (index scan, shard scan, nested loop, full scan)
"""

# Query Cost Constants
# Based on typical cloud infrastructure parameters

# Server specifications
DOCS_PER_SERVER = 4_000_000  # Average documents per server
MEMORY_PER_SERVER_GB = 256   # Server memory

# Query performance (operations per ms)
INDEX_LOOKUP_TIME_MS = 0.001        # Direct index access
SHARD_SCAN_TIME_PER_DOC_MS = 0.00001  # Document scan on single server
FULL_SCAN_TIME_PER_DOC_MS = 0.00002   # Full cluster scan (network overhead)

# Power consumption
SERVER_IDLE_POWER_W = 200        # Watts when idle
SERVER_ACTIVE_POWER_W = 400      # Watts under load
NETWORK_POWER_W_PER_GBIT = 50    # Network power per gigabit

# Cost parameters
CLOUD_COST_PER_HOUR_PER_SERVER = 0.50  # USD per hour per server
CO2_PER_KWH = 0.233  # kg CO2 per kilowatt-hour (average grid)


class QueryCostAnalyzer:
    """
    Analyzes the cost of a query execution.
    
    Attributes:
        documents_scanned (int): Number of documents to scan
        distinct_values (int): Number of distinct values for sharding key
        nb_servers (int): Total servers in cluster
        algorithm (str): Query algorithm ('index', 'shard', 'nested_loop', 'full_scan')
    """
    
    def __init__(self, documents_scanned, distinct_values, nb_servers, algorithm="shard"):
        """
        Initialize query cost analyzer.
        
        Args:
            documents_scanned (int): Documents to scan
            distinct_values (int): Distinct key values
            nb_servers (int): Total cluster servers
            algorithm (str): 'index', 'shard', 'nested_loop', or 'full_scan'
        """
        self.documents_scanned = documents_scanned
        self.distinct_values = distinct_values
        self.nb_servers = nb_servers
        self.algorithm = algorithm.lower()
        
        # Validate algorithm
        if self.algorithm not in ['index', 'shard', 'nested_loop', 'full_scan']:
            raise ValueError(f"Unknown algorithm: {algorithm}")
    
    def compute_servers_accessed(self):
        """
        Estimate number of servers that need to be accessed.
        
        Returns:
            int: Number of servers accessed
        """
        if self.algorithm == 'index':
            # Index: direct access, minimal servers
            return 1
        elif self.algorithm == 'shard':
            # Shard: based on distinct values
            servers = max(1, self.distinct_values // (DOCS_PER_SERVER // 100))
            return min(servers, self.nb_servers)
        elif self.algorithm == 'nested_loop':
            # Nested loop: scan all documents, distributed
            return min(max(1, self.documents_scanned // DOCS_PER_SERVER), self.nb_servers)
        elif self.algorithm == 'full_scan':
            # Full scan: access all servers
            return self.nb_servers
        
        return 1
    
    def compute_time_ms(self):
        """
        Estimate query execution time in milliseconds.
        
        Returns:
            float: Execution time in milliseconds
        """
        if self.algorithm == 'index':
            # Index lookup: very fast
            return INDEX_LOOKUP_TIME_MS * self.documents_scanned
        
        elif self.algorithm == 'shard':
            # Shard scan: scan on relevant servers
            servers_accessed = self.compute_servers_accessed()
            docs_per_server = self.documents_scanned / servers_accessed if servers_accessed > 0 else 0
            return docs_per_server * SHARD_SCAN_TIME_PER_DOC_MS
        
        elif self.algorithm == 'nested_loop':
            # Nested loop: slower, but distributed
            return self.documents_scanned * SHARD_SCAN_TIME_PER_DOC_MS * 2
        
        elif self.algorithm == 'full_scan':
            # Full scan: slowest
            return self.documents_scanned * FULL_SCAN_TIME_PER_DOC_MS
        
        return 0.0
    
    def compute_carbon_footprint_kg(self):
        """
        Estimate carbon footprint in kg CO2.
        
        Based on:
        - Number of servers accessed
        - Query duration
        - Power consumption
        
        Returns:
            float: Carbon footprint in kg CO2
        """
        servers_accessed = self.compute_servers_accessed()
        time_seconds = self.compute_time_ms() / 1000.0
        
        # Energy consumption
        power_w = servers_accessed * SERVER_ACTIVE_POWER_W + NETWORK_POWER_W_PER_GBIT
        energy_kwh = (power_w / 1000.0) * (time_seconds / 3600.0)
        
        # Carbon emissions
        carbon_kg = energy_kwh * CO2_PER_KWH
        
        return carbon_kg
    
    def compute_price_usd(self):
        """
        Estimate query cost in USD.
        
        Based on:
        - Number of servers accessed
        - Query duration
        - Cloud provider hourly rates
        
        Returns:
            float: Query cost in USD
        """
        servers_accessed = self.compute_servers_accessed()
        time_seconds = self.compute_time_ms() / 1000.0
        
        # Compute cost
        hours = time_seconds / 3600.0
        cost = servers_accessed * CLOUD_COST_PER_HOUR_PER_SERVER * hours
        
        return cost
    
    def get_summary(self):
        """
        Get complete cost summary.
        
        Returns:
            dict: Dictionary with time, carbon, price, and servers accessed
        """
        return {
            'algorithm': self.algorithm,
            'servers_accessed': self.compute_servers_accessed(),
            'time_ms': self.compute_time_ms(),
            'carbon_kg': self.compute_carbon_footprint_kg(),
            'price_usd': self.compute_price_usd()
        }


def compare_algorithms(documents_scanned, distinct_values, nb_servers):
    """
    Compare all algorithms for a given query pattern.
    
    Args:
        documents_scanned (int): Documents to scan
        distinct_values (int): Distinct key values
        nb_servers (int): Total servers
    
    Returns:
        dict: Comparison of all algorithms
    """
    algorithms = ['index', 'shard', 'nested_loop', 'full_scan']
    results = {}
    
    for algo in algorithms:
        analyzer = QueryCostAnalyzer(documents_scanned, distinct_values, nb_servers, algo)
        results[algo] = analyzer.get_summary()
    
    return results
