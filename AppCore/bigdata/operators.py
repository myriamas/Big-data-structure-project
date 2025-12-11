"""
Operator implementations for DVL package submission.

Provides four operators:
- filter_with_sharding
- filter_without_sharding
- nested_loop_with_sharding
- nested_loop_without_sharding

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
