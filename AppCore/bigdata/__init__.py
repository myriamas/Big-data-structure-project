"""
Big Data Structure - Storage Estimator Package

A comprehensive package for analyzing NoSQL denormalization strategies,
computing storage costs, and evaluating sharding distribution.

Main components:
- models.py: Collection and data models
- sizes.py: Size computation (documents, collections, databases)
- sharding.py: Sharding distribution analysis
- query_costs.py: Query cost analysis (time, carbon, price)
- queries.py: Query execution simulation
- operators.py: DVL operators (filter, nested-loop with/without sharding)
"""

from .models import Collection
from .sizes import (
    compute_document_size_from_schema,
    compute_collection_size_bytes,
    compute_database_size_bytes,
    compute_database_size_gb,
    bytes_to_gb
)
from .sharding import compute_sharding_distribution
from .query_costs import QueryCostAnalyzer
from .operators import (
    filter_with_sharding,
    filter_without_sharding,
    nested_loop_with_sharding,
    nested_loop_without_sharding,
)

__version__ = "1.0.0"
__author__ = "BDS Project Team"

__all__ = [
    "Collection",
    "compute_document_size_from_schema",
    "compute_collection_size_bytes",
    "compute_database_size_bytes",
    "compute_database_size_gb",
    "bytes_to_gb",
    "compute_sharding_distribution",
    "QueryCostAnalyzer",
    "filter_with_sharding",
    "filter_without_sharding",
    "nested_loop_with_sharding",
    "nested_loop_without_sharding",
]
