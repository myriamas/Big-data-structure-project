"""Unit tests for operators module.

Tests the four operators (filter with/without sharding and nested-loop with/without sharding)
to verify cost computation, output counts, and edge cases.
"""

import unittest
import json
import os
import sys

# Add project root to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from AppCore.bigdata.models import Collection
from AppCore.bigdata.operators import (
    filter_with_sharding,
    filter_without_sharding,
    nested_loop_with_sharding,
    nested_loop_without_sharding,
)


class TestFilterOperators(unittest.TestCase):
    """Test filter operators."""
    
    @classmethod
    def setUpClass(cls):
        """Load schemas and stats once."""
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'schema'))
        stats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json'))
        
        cls.stats = {}
        if os.path.exists(stats_path):
            with open(stats_path, 'r', encoding='utf-8') as f:
                cls.stats = json.load(f)
        
        with open(os.path.join(base, 'product_schema.json'), 'r', encoding='utf-8') as f:
            cls.product_schema = json.load(f)
        
        cls.product_collection = Collection(
            'product',
            cls.product_schema,
            cls.stats,
            document_count=cls.stats.get('nb_products', 100000)
        )
    
    def test_filter_with_sharding_basic(self):
        """Test filter_with_sharding returns expected keys."""
        result = filter_with_sharding(
            self.product_collection,
            expected_output_keys=['name', 'price'],
            filtered_key='brand'
        )
        
        # Check structure
        self.assertIn('operator', result)
        self.assertEqual(result['operator'], 'filter_with_sharding')
        self.assertIn('output_docs', result)
        self.assertIn('output_bytes', result)
        self.assertIn('cost', result)
        
        # Output should be less than input
        self.assertLess(result['output_docs'], self.product_collection.document_count)
        
        # Cost should have expected keys
        self.assertIn('algorithm', result['cost'])
        self.assertEqual(result['cost']['algorithm'], 'shard')
    
    def test_filter_without_sharding_basic(self):
        """Test filter_without_sharding returns expected keys."""
        result = filter_without_sharding(
            self.product_collection,
            expected_output_keys=['name', 'price'],
            filtered_key='brand'
        )
        
        self.assertIn('operator', result)
        self.assertEqual(result['operator'], 'filter_without_sharding')
        self.assertIn('cost', result)
        self.assertEqual(result['cost']['algorithm'], 'full_scan')
    
    def test_selectivity_provided(self):
        """Test that provided selectivity is used."""
        custom_sel = 0.1
        result = filter_with_sharding(
            self.product_collection,
            expected_output_keys=['name'],
            filtered_key='brand',
            selectivity=custom_sel
        )
        
        self.assertAlmostEqual(result['selectivity'], custom_sel)
        self.assertAlmostEqual(
            result['output_docs'],
            int(self.product_collection.document_count * custom_sel)
        )
    
    def test_sharding_vs_no_sharding_cost_difference(self):
        """Sharded filter should be cheaper than unsharded."""
        sharded = filter_with_sharding(
            self.product_collection,
            expected_output_keys=['name'],
            filtered_key='brand'
        )
        
        unsharded = filter_without_sharding(
            self.product_collection,
            expected_output_keys=['name'],
            filtered_key='brand'
        )
        
        # Sharded should have lower time and cost
        self.assertLess(sharded['cost']['time_ms'], unsharded['cost']['time_ms'])
        self.assertLess(sharded['cost']['price_usd'], unsharded['cost']['price_usd'])


class TestNestedLoopOperators(unittest.TestCase):
    """Test nested-loop join operators."""
    
    @classmethod
    def setUpClass(cls):
        """Load schemas and stats once."""
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'schema'))
        stats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json'))
        
        cls.stats = {}
        if os.path.exists(stats_path):
            with open(stats_path, 'r', encoding='utf-8') as f:
                cls.stats = json.load(f)
        
        with open(os.path.join(base, 'product_schema.json'), 'r', encoding='utf-8') as f:
            cls.product_schema = json.load(f)
        
        with open(os.path.join(base, 'db3_stock_schema.json'), 'r', encoding='utf-8') as f:
            cls.stock_schema = json.load(f)
        
        cls.product_collection = Collection(
            'product',
            cls.product_schema,
            cls.stats,
            document_count=cls.stats.get('nb_products', 100000)
        )
        
        cls.stock_collection = Collection(
            'stock',
            cls.stock_schema,
            cls.stats,
            document_count=cls.stats.get('nb_products', 100000) * cls.stats.get('nb_warehouses', 200)
        )
    
    def test_nested_loop_with_sharding_basic(self):
        """Test nested_loop_with_sharding returns expected keys."""
        result = nested_loop_with_sharding(
            self.product_collection,
            self.stock_collection,
            expected_output_keys=['name', 'price', 'quantity'],
            filtered_key='brand'
        )
        
        self.assertIn('operator', result)
        self.assertEqual(result['operator'], 'nested_loop_with_sharding')
        self.assertIn('docs_scanned', result)
        self.assertIn('output_docs', result)
        self.assertIn('cost', result)
    
    def test_nested_loop_without_sharding_basic(self):
        """Test nested_loop_without_sharding returns expected keys."""
        result = nested_loop_without_sharding(
            self.product_collection,
            self.stock_collection,
            expected_output_keys=['name', 'price', 'quantity'],
            filtered_key='brand'
        )
        
        self.assertIn('operator', result)
        self.assertEqual(result['operator'], 'nested_loop_without_sharding')
        self.assertIn('cost', result)
    
    def test_nested_loop_sharding_vs_no_sharding(self):
        """Sharded nested-loop should be cheaper than unsharded."""
        sharded = nested_loop_with_sharding(
            self.product_collection,
            self.stock_collection,
            expected_output_keys=['name'],
            filtered_key='brand'
        )
        
        unsharded = nested_loop_without_sharding(
            self.product_collection,
            self.stock_collection,
            expected_output_keys=['name'],
            filtered_key='brand'
        )
        
        # Sharded should scan fewer docs and cost less
        self.assertLess(sharded['docs_scanned'], unsharded['docs_scanned'])
        self.assertLess(sharded['cost']['price_usd'], unsharded['cost']['price_usd'])


class TestOutputSizeComputation(unittest.TestCase):
    """Test output size computation."""
    
    @classmethod
    def setUpClass(cls):
        """Load schemas and stats."""
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'schema'))
        stats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json'))
        
        cls.stats = {}
        if os.path.exists(stats_path):
            with open(stats_path, 'r', encoding='utf-8') as f:
                cls.stats = json.load(f)
        
        with open(os.path.join(base, 'product_schema.json'), 'r', encoding='utf-8') as f:
            cls.product_schema = json.load(f)
        
        cls.product_collection = Collection(
            'product',
            cls.product_schema,
            cls.stats,
            document_count=cls.stats.get('nb_products', 100000)
        )
    
    def test_output_bytes_proportional_to_docs(self):
        """Output bytes should scale with output docs."""
        result1 = filter_with_sharding(
            self.product_collection,
            expected_output_keys=['name'],
            filtered_key='brand',
            selectivity=0.05
        )
        
        result2 = filter_with_sharding(
            self.product_collection,
            expected_output_keys=['name'],
            filtered_key='brand',
            selectivity=0.10
        )
        
        # Higher selectivity should produce more docs and more bytes
        self.assertGreater(result2['output_docs'], result1['output_docs'])
        self.assertGreater(result2['output_bytes'], result1['output_bytes'])


if __name__ == '__main__':
    unittest.main()
