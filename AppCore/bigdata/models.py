"""
Data models for the Big Data Structure Storage Estimator

This module provides the core data structures for representing
collections in a NoSQL database system.
"""


class Collection:
    """
    Represents a NoSQL collection with its metadata and statistics.
    
    A Collection encapsulates all information needed to analyze storage costs
    and sharding distribution for a given collection in a database.
    
    Attributes:
        name (str): Name of the collection (e.g., "Product", "Stock")
        schema (dict): JSON Schema describing the collection structure
        stats (dict): Global statistics (nb_products, nb_servers, etc.)
        document_count (int): Number of documents in this collection
    """
    
    def __init__(self, name, schema, stats, document_count):
        """
        Initialize a Collection object.
        
        Args:
            name (str): Collection name
            schema (dict): JSON Schema object
            stats (dict): Dictionary of global statistics
            document_count (int): Number of documents in this collection
            
        Raises:
            ValueError: If document_count is negative
            TypeError: If schema is not a dictionary
        """
        if not isinstance(schema, dict):
            raise TypeError("schema must be a dictionary (JSON Schema)")
        if document_count < 0:
            raise ValueError("document_count must be non-negative")
        
        self.name = name
        self.schema = schema
        self.stats = stats
        self.document_count = document_count
    
    def __repr__(self):
        """String representation of the Collection."""
        return f"Collection(name='{self.name}', documents={self.document_count})"
    
    def __str__(self):
        """User-friendly string representation."""
        return f"{self.name}: {self.document_count} documents"

