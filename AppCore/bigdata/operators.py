"""
Operator implementations for DVL package submission.

Provides six operators:
- filter_with_sharding
- filter_without_sharding
- nested_loop_with_sharding
- nested_loop_without_sharding
- aggregate_with_sharding
- aggregate_without_sharding

Each operator accepts Collection objects defined in `models.Collection`,
an `expected_output_keys` list, a `filtered_key` string and an optional
`selectivity` float (between 0 and 1). They return a dictionary with:
- number of output documents and bytes
- a cost summary (time, carbon, price, servers accessed)

This module reuses the project's sizing and cost utilities.
"""
from math import ceil
from typing import Dict, Any, List, Optional

from .models import Collection
from .sizes import compute_document_size_from_schema
from .query_costs import QueryCostAnalyzer


def _estimate_selectivity(collection: Collection, key: str, selectivity: Optional[float], stats: Dict[str, Any]) -> float:
    """Estimate selectivity for a given key when not provided.

    Heuristics:
    - If the key is `brand` and stats contain `nb_apple_products`/`nb_products`, use that ratio.
    - If stats contain `distinct_brands` or `distinct_<key>`, inverse of that.
    - Otherwise default to 0.01 (1%).
    """
    if selectivity is not None:
        return float(max(0.0, min(1.0, selectivity)))

    # Try heuristics from known stats keys
    nb_products = stats.get('nb_products') or stats.get('num_products')
    nb_apple = stats.get('nb_apple_products') or stats.get('num_apple_products')
    distinct_brands = stats.get('distinct_brands') or stats.get('distinct_brands')

    if key.lower() == 'brand' and nb_products and nb_apple:
        return max(1e-6, nb_apple / float(nb_products))

    if distinct_brands:
        try:
            return max(1e-6, 1.0 / float(distinct_brands))
        except Exception:
            pass

    # Generic default
    return 0.01


def _compute_output_size_bytes(collection: Collection, output_docs: int) -> int:
    doc_size = compute_document_size_from_schema(collection.schema)
    return int(doc_size * output_docs)


def filter_with_sharding(collection: Collection, expected_output_keys: List[str], filtered_key: str,
                         selectivity: Optional[float] = None) -> Dict[str, Any]:
    """Filter operator assuming sharding on the filtered key (efficient).

    Returns dict with document counts, size bytes and cost summary.
    """
    stats = collection.stats or {}
    sel = _estimate_selectivity(collection, filtered_key, selectivity, stats)
    output_docs = max(0, int(ceil(collection.document_count * sel)))

    # Documents scanned: ideally only matching documents are read (index/shard routing)
    documents_scanned = output_docs

    # Distinct values estimate for sharding cost model
    distinct_values = max(1, int(1.0 / sel))
    nb_servers = stats.get('nb_servers') or stats.get('nbServers') or 1000

    analyzer = QueryCostAnalyzer(documents_scanned, distinct_values, nb_servers, algorithm='shard')
    cost = analyzer.get_summary()

    return {
        'operator': 'filter_with_sharding',
        'collection': collection.name,
        'filtered_key': filtered_key,
        'selectivity': sel,
        'output_docs': output_docs,
        'output_bytes': _compute_output_size_bytes(collection, output_docs),
        'cost': cost
    }


def filter_without_sharding(collection: Collection, expected_output_keys: List[str], filtered_key: str,
                            selectivity: Optional[float] = None) -> Dict[str, Any]:
    """Filter operator when no sharding or no index is available (full scan).

    We assume the database must scan the entire collection to filter.
    """
    stats = collection.stats or {}
    sel = _estimate_selectivity(collection, filtered_key, selectivity, stats)

    # Full scan: the engine scans all documents, output is subset
    documents_scanned = collection.document_count
    output_docs = max(0, int(ceil(collection.document_count * sel)))

    distinct_values = max(1, int(1.0 / sel))
    nb_servers = stats.get('nb_servers') or 1000

    analyzer = QueryCostAnalyzer(documents_scanned, distinct_values, nb_servers, algorithm='full_scan')
    cost = analyzer.get_summary()

    return {
        'operator': 'filter_without_sharding',
        'collection': collection.name,
        'filtered_key': filtered_key,
        'selectivity': sel,
        'output_docs': output_docs,
        'output_bytes': _compute_output_size_bytes(collection, output_docs),
        'cost': cost
    }


def nested_loop_with_sharding(left: Collection, right: Collection, expected_output_keys: List[str],
                              filtered_key: str, selectivity: Optional[float] = None,
                              stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Nested-loop join assuming sharding that helps reduce scanned data.

    We assume the outer collection is filtered by `filtered_key` (selectivity applies to left).
    """
    stats = stats or left.stats or {}
    sel = _estimate_selectivity(left, filtered_key, selectivity, stats)
    outer_filtered = max(0, int(ceil(left.document_count * sel)))

    # Estimate average matches per outer row in right collection
    nb_warehouses = stats.get('nb_warehouses') or stats.get('nb_warehouses', 200)
    try:
        avg_matches = max(1, int(ceil(right.document_count / float(nb_warehouses))))
    except Exception:
        avg_matches = 1

    docs_scanned = outer_filtered * avg_matches

    # Prevent runaway numbers; clamp to a reasonable upper bound using total documents
    max_possible = left.document_count + right.document_count
    docs_scanned = min(docs_scanned, max_possible)

    output_docs = outer_filtered * avg_matches
    output_docs = min(output_docs, left.document_count * right.document_count)

    distinct_values = max(1, int(1.0 / sel))
    nb_servers = stats.get('nb_servers') or 1000

    analyzer = QueryCostAnalyzer(docs_scanned, distinct_values, nb_servers, algorithm='nested_loop')
    cost = analyzer.get_summary()

    return {
        'operator': 'nested_loop_with_sharding',
        'left_collection': left.name,
        'right_collection': right.name,
        'filtered_key': filtered_key,
        'selectivity': sel,
        'docs_scanned': docs_scanned,
        'output_docs': output_docs,
        'output_bytes': _compute_output_size_bytes(left, output_docs),
        'cost': cost
    }


def nested_loop_without_sharding(left: Collection, right: Collection, expected_output_keys: List[str],
                                 filtered_key: str, selectivity: Optional[float] = None,
                                 stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Nested-loop join without helpful sharding: more expensive (full scans).
    """
    stats = stats or left.stats or {}
    sel = _estimate_selectivity(left, filtered_key, selectivity, stats)
    outer_filtered = max(0, int(ceil(left.document_count * sel)))

    # Without sharding we may need to scan the full inner collection for each outer row
    docs_scanned = outer_filtered * right.document_count

    # Clamp to avoid unrealistic numbers
    max_possible = left.document_count * right.document_count
    docs_scanned = min(docs_scanned, max_possible)

    output_docs = outer_filtered * (right.document_count // max(1, stats.get('nb_warehouses', 200)))

    distinct_values = max(1, int(1.0 / sel))
    nb_servers = stats.get('nb_servers') or 1000

    analyzer = QueryCostAnalyzer(docs_scanned, distinct_values, nb_servers, algorithm='nested_loop')
    cost = analyzer.get_summary()

    return {
        'operator': 'nested_loop_without_sharding',
        'left_collection': left.name,
        'right_collection': right.name,
        'filtered_key': filtered_key,
        'selectivity': sel,
        'docs_scanned': docs_scanned,
        'output_docs': output_docs,
        'output_bytes': _compute_output_size_bytes(left, output_docs),
        'cost': cost
    }


# =============================================================================
# Aggregate Operators (Part 4 - Homework 4.2)
# =============================================================================

def _estimate_num_groups(collection: Collection, group_key: str, stats: Dict[str, Any]) -> int:
    """Estimate the number of distinct groups for a GROUP BY key.

    Uses statistics to determine cardinality:
    - brand -> distinct_brands
    - warehouse_id/IDW -> nb_warehouses
    - client_id/IDC -> nb_clients
    - product_id/IDP -> nb_products
    - Default: document_count // 1000
    """
    key_lower = group_key.lower()

    # Map known keys to stats
    if key_lower in ('brand',):
        return stats.get('distinct_brands', 5000)
    elif key_lower in ('warehouse_id', 'idw', 'warehouse'):
        return stats.get('nb_warehouses', 200)
    elif key_lower in ('client_id', 'idc', 'client', 'idclient'):
        return stats.get('nb_clients', 10_000_000)
    elif key_lower in ('product_id', 'idp', 'product', 'idproduct'):
        return stats.get('nb_products', 100_000)
    elif key_lower in ('date', 'order_date'):
        # Order lines balanced over 365 dates
        return 365
    else:
        # Fallback: assume moderate cardinality
        return max(1, collection.document_count // 1000)


def _compute_aggregate_output_size(num_groups: int, aggregate_functions: List[str]) -> int:
    """Compute output size for aggregate results.

    Each group produces one output record with:
    - Group key: ~50 bytes
    - Each aggregate value: ~20 bytes (number + overhead)
    """
    key_size = 50  # Group key
    agg_size = 20 * len(aggregate_functions)  # Aggregate values
    overhead = 12  # JSON overhead per record
    return num_groups * (key_size + agg_size + overhead)


def aggregate_with_sharding(collection: Collection, group_key: str,
                            aggregate_functions: List[str],
                            filter_key: Optional[str] = None,
                            selectivity: Optional[float] = None) -> Dict[str, Any]:
    """Aggregate operator with Map/Reduce optimization using sharding.

    Implements a distributed aggregation with three phases:
    1. Map: Parallel scan on sharded data
    2. Shuffle: Redistribute intermediate results by group key
    3. Reduce: Final aggregation on reducer servers

    Args:
        collection: The collection to aggregate
        group_key: The key to GROUP BY
        aggregate_functions: List of aggregates ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']
        filter_key: Optional filter to apply before aggregation
        selectivity: Optional selectivity for the filter (0.0-1.0)

    Returns:
        Dictionary with operator results and costs
    """
    stats = collection.stats or {}
    nb_servers = stats.get('nb_servers', 1000)

    # Determine documents to scan (apply filter if specified)
    if filter_key:
        sel = _estimate_selectivity(collection, filter_key, selectivity, stats)
        docs_scanned = max(1, int(ceil(collection.document_count * sel)))
    else:
        sel = 1.0
        docs_scanned = collection.document_count

    # Estimate number of output groups
    num_groups = _estimate_num_groups(collection, group_key, stats)

    # If filtering reduces cardinality, adjust groups proportionally
    if filter_key and sel < 1.0:
        num_groups = max(1, int(num_groups * sel))

    # Map Phase: Parallel scan across servers
    # With good sharding, work is distributed evenly
    docs_per_server = docs_scanned / nb_servers
    map_time_ms = docs_per_server * 0.00001  # Fast parallel scan

    # Shuffle Phase: Network transfer of intermediate results
    # Each server sends partial aggregates for each group
    intermediate_record_size = 100  # bytes per (group_key, partial_agg)
    shuffle_data_bytes = num_groups * intermediate_record_size
    shuffle_time_ms = (shuffle_data_bytes / (1_000_000)) * 0.5  # Network transfer

    # Reduce Phase: Final aggregation
    num_reducers = min(num_groups, nb_servers)
    groups_per_reducer = max(1, num_groups // num_reducers)
    reduce_time_ms = groups_per_reducer * 0.0001  # Aggregation per group

    # Total time
    total_time_ms = map_time_ms + shuffle_time_ms + reduce_time_ms

    # Servers accessed: all for map, subset for reduce
    servers_accessed = min(nb_servers, max(1, docs_scanned // 4_000_000) + num_reducers)

    # Use QueryCostAnalyzer for consistent cost computation
    analyzer = QueryCostAnalyzer(docs_scanned, num_groups, nb_servers, algorithm='shard')
    base_cost = analyzer.get_summary()

    # Adjust time to include all phases
    time_ratio = total_time_ms / max(0.0001, base_cost['time_ms'])
    adjusted_carbon = base_cost['carbon_kg'] * time_ratio
    adjusted_price = base_cost['price_usd'] * time_ratio

    return {
        'operator': 'aggregate_with_sharding',
        'collection': collection.name,
        'group_key': group_key,
        'aggregate_functions': aggregate_functions,
        'filter_key': filter_key,
        'selectivity': sel,
        'docs_scanned': docs_scanned,
        'num_groups': num_groups,
        'output_docs': num_groups,
        'output_bytes': _compute_aggregate_output_size(num_groups, aggregate_functions),
        'cost': {
            'algorithm': 'map_reduce',
            'servers_accessed': servers_accessed,
            'time_ms': total_time_ms,
            'carbon_kg': adjusted_carbon,
            'price_usd': adjusted_price,
            'phases': {
                'map_time_ms': map_time_ms,
                'shuffle_time_ms': shuffle_time_ms,
                'reduce_time_ms': reduce_time_ms
            }
        }
    }


def aggregate_without_sharding(collection: Collection, group_key: str,
                               aggregate_functions: List[str],
                               filter_key: Optional[str] = None,
                               selectivity: Optional[float] = None) -> Dict[str, Any]:
    """Aggregate operator without sharding optimization (full scan).

    All servers must be accessed, and aggregation happens sequentially
    without the benefit of data locality.

    Args:
        collection: The collection to aggregate
        group_key: The key to GROUP BY
        aggregate_functions: List of aggregates ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']
        filter_key: Optional filter to apply before aggregation
        selectivity: Optional selectivity for the filter (0.0-1.0)

    Returns:
        Dictionary with operator results and costs
    """
    stats = collection.stats or {}
    nb_servers = stats.get('nb_servers', 1000)

    # Determine documents to scan (apply filter if specified)
    if filter_key:
        sel = _estimate_selectivity(collection, filter_key, selectivity, stats)
        docs_after_filter = max(1, int(ceil(collection.document_count * sel)))
    else:
        sel = 1.0
        docs_after_filter = collection.document_count

    # Full scan required - all documents must be read
    docs_scanned = collection.document_count

    # Estimate number of output groups
    num_groups = _estimate_num_groups(collection, group_key, stats)

    # If filtering reduces cardinality, adjust groups proportionally
    if filter_key and sel < 1.0:
        num_groups = max(1, int(num_groups * sel))

    # Without sharding: sequential full scan
    scan_time_ms = docs_scanned * 0.00002  # Full scan time per doc

    # Shuffle: Still need to redistribute, but less efficiently
    intermediate_record_size = 100
    shuffle_data_bytes = num_groups * intermediate_record_size * nb_servers
    shuffle_time_ms = (shuffle_data_bytes / (1_000_000)) * 1.0  # Slower network

    # Reduce: Sequential aggregation
    reduce_time_ms = num_groups * 0.001  # Slower without parallelism

    # Total time (significantly higher than with sharding)
    total_time_ms = scan_time_ms + shuffle_time_ms + reduce_time_ms

    # All servers accessed for full scan
    servers_accessed = nb_servers

    # Use QueryCostAnalyzer for consistent cost computation
    analyzer = QueryCostAnalyzer(docs_scanned, num_groups, nb_servers, algorithm='full_scan')
    base_cost = analyzer.get_summary()

    # Adjust costs
    time_ratio = total_time_ms / max(0.0001, base_cost['time_ms'])
    adjusted_carbon = base_cost['carbon_kg'] * time_ratio
    adjusted_price = base_cost['price_usd'] * time_ratio

    return {
        'operator': 'aggregate_without_sharding',
        'collection': collection.name,
        'group_key': group_key,
        'aggregate_functions': aggregate_functions,
        'filter_key': filter_key,
        'selectivity': sel,
        'docs_scanned': docs_scanned,
        'docs_after_filter': docs_after_filter,
        'num_groups': num_groups,
        'output_docs': num_groups,
        'output_bytes': _compute_aggregate_output_size(num_groups, aggregate_functions),
        'cost': {
            'algorithm': 'full_scan_aggregate',
            'servers_accessed': servers_accessed,
            'time_ms': total_time_ms,
            'carbon_kg': adjusted_carbon,
            'price_usd': adjusted_price,
            'phases': {
                'scan_time_ms': scan_time_ms,
                'shuffle_time_ms': shuffle_time_ms,
                'reduce_time_ms': reduce_time_ms
            }
        }
    }
