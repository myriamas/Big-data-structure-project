def compute_sharding_distribution(document_count, distinct_key_values, nb_servers):
    avg_docs_per_server = document_count / nb_servers
    avg_distinct_values_per_server = distinct_key_values / nb_servers
    return {
        "avg_docs_per_server": avg_docs_per_server,
        "avg_distinct_values_per_server": avg_distinct_values_per_server
    }
