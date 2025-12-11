"""Distribution de sharding
compute_sharding_distribution() : 
-Répartit documents/clés entre serveurs
-Calcule moyenne docs et clés distinctes par serveur"""

"""
Sharding distribution analysis module.

This module provides functions to analyze how documents would be distributed
across multiple servers using a sharding strategy.

A sharding strategy partitions documents into buckets (servers) based on a key.
This module calculates statistics about the distribution quality.
"""


def compute_sharding_distribution(document_count, distinct_key_values, nb_servers):
    """
    Compute sharding distribution statistics.
    
    Analyzes how documents would be distributed across servers if using
    a specific sharding key.
    
    Args:
        document_count (int): Total number of documents in the collection
        distinct_key_values (int): Number of distinct values for the sharding key
        nb_servers (int): Total number of servers in the cluster
    
    Returns:
        dict: Dictionary containing:
            - avg_docs_per_server (float): Average documents per server
            - avg_distinct_values_per_server (float): Average distinct key values per server
    
    Example:
        >>> result = compute_sharding_distribution(
        ...     document_count=20_000_000,
        ...     distinct_key_values=100_000,
        ...     nb_servers=1000
        ... )
        >>> result['avg_docs_per_server']
        20000.0
        >>> result['avg_distinct_values_per_server']
        100.0
    
    Notes:
        - A good sharding strategy has many distinct key values relative to servers
        - If distinct_key_values < nb_servers, many servers will be empty (hotspots)
        - Ideal: distinct_key_values >> nb_servers for uniform distribution
    """
    avg_docs_per_server = document_count / nb_servers
    avg_distinct_values_per_server = distinct_key_values / nb_servers
    
    return {
        "avg_docs_per_server": avg_docs_per_server,
        "avg_distinct_values_per_server": avg_distinct_values_per_server
    }

