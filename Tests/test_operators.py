"""Unit tests for operators module.

Tests the six operators (filter, nested-loop, aggregate with/without sharding)
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
    aggregate_with_sharding,
    aggregate_without_sharding,
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


class TestAggregateOperators(unittest.TestCase):
    """Test aggregate operators (Part 4)."""

    @classmethod
    def setUpClass(cls):
        """Load schemas and stats once."""
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'schema'))
        stats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'AppCore', 'stats.json'))

        cls.stats = {}
        if os.path.exists(stats_path):
            with open(stats_path, 'r', encoding='utf-8') as f:
                cls.stats = json.load(f)

        # Load orderline schema if available, else use a simple one
        orderline_path = os.path.join(base, 'db4_orderline_schema.json')
        if os.path.exists(orderline_path):
            with open(orderline_path, 'r', encoding='utf-8') as f:
                cls.orderline_schema = json.load(f)
        else:
            cls.orderline_schema = {
                'properties': {
                    'idp': {'type': 'integer'},
                    'idc': {'type': 'integer'},
                    'quantity': {'type': 'integer'},
                    'date': {'type': 'string'}
                }
            }

        with open(os.path.join(base, 'product_schema.json'), 'r', encoding='utf-8') as f:
            cls.product_schema = json.load(f)

        cls.orderline_collection = Collection(
            'orderline',
            cls.orderline_schema,
            cls.stats,
            document_count=cls.stats.get('nb_orderlines', 4_000_000_000)
        )

        cls.product_collection = Collection(
            'product',
            cls.product_schema,
            cls.stats,
            document_count=cls.stats.get('nb_products', 100000)
        )

    def test_aggregate_with_sharding_basic(self):
        """Test aggregate_with_sharding returns expected keys."""
        result = aggregate_with_sharding(
            self.orderline_collection,
            group_key='idp',
            aggregate_functions=['SUM', 'COUNT']
        )

        # Check structure
        self.assertIn('operator', result)
        self.assertEqual(result['operator'], 'aggregate_with_sharding')
        self.assertIn('group_key', result)
        self.assertIn('num_groups', result)
        self.assertIn('output_docs', result)
        self.assertIn('output_bytes', result)
        self.assertIn('cost', result)

        # Output docs should equal num_groups (one per group)
        self.assertEqual(result['output_docs'], result['num_groups'])

        # Cost should have expected keys
        self.assertIn('algorithm', result['cost'])
        self.assertEqual(result['cost']['algorithm'], 'map_reduce')
        self.assertIn('phases', result['cost'])

    def test_aggregate_without_sharding_basic(self):
        """Test aggregate_without_sharding returns expected keys."""
        result = aggregate_without_sharding(
            self.product_collection,
            group_key='brand',
            aggregate_functions=['COUNT']
        )

        self.assertIn('operator', result)
        self.assertEqual(result['operator'], 'aggregate_without_sharding')
        self.assertIn('cost', result)
        self.assertEqual(result['cost']['algorithm'], 'full_scan_aggregate')

    def test_aggregate_num_groups_estimation(self):
        """Test that group cardinality is estimated correctly."""
        # Group by brand should have ~5000 groups
        result_brand = aggregate_with_sharding(
            self.product_collection,
            group_key='brand',
            aggregate_functions=['COUNT']
        )
        self.assertEqual(result_brand['num_groups'], self.stats.get('distinct_brands', 5000))

        # Group by product ID should have ~100K groups
        result_idp = aggregate_with_sharding(
            self.orderline_collection,
            group_key='idp',
            aggregate_functions=['SUM']
        )
        self.assertEqual(result_idp['num_groups'], self.stats.get('nb_products', 100000))

    def test_aggregate_with_filter(self):
        """Test aggregate with filter reduces groups."""
        # Without filter
        result_no_filter = aggregate_with_sharding(
            self.orderline_collection,
            group_key='idp',
            aggregate_functions=['SUM']
        )

        # With filter (e.g., specific client)
        result_filtered = aggregate_with_sharding(
            self.orderline_collection,
            group_key='idp',
            aggregate_functions=['SUM'],
            filter_key='idc',
            selectivity=0.0001  # Very selective
        )

        # Filtered should scan fewer docs
        self.assertLess(result_filtered['docs_scanned'], result_no_filter['docs_scanned'])

    def test_aggregate_sharding_vs_no_sharding_cost(self):
        """Sharded aggregate should be cheaper than unsharded."""
        sharded = aggregate_with_sharding(
            self.product_collection,
            group_key='brand',
            aggregate_functions=['COUNT', 'SUM']
        )

        unsharded = aggregate_without_sharding(
            self.product_collection,
            group_key='brand',
            aggregate_functions=['COUNT', 'SUM']
        )

        # Sharded should have lower time and cost
        self.assertLess(sharded['cost']['time_ms'], unsharded['cost']['time_ms'])
        self.assertLess(sharded['cost']['price_usd'], unsharded['cost']['price_usd'])

    def test_aggregate_output_size_small(self):
        """Aggregate output should be much smaller than input."""
        result = aggregate_with_sharding(
            self.orderline_collection,
            group_key='idp',
            aggregate_functions=['SUM']
        )

        # Output should be much smaller than scanning 4B docs
        # Even with 100K groups, output bytes should be reasonable
        self.assertLess(result['output_bytes'], 100_000_000)  # Less than 100MB
        self.assertLess(result['output_docs'], self.orderline_collection.document_count)

    def test_aggregate_phases_present(self):
        """Test that all Map/Reduce phases are reported."""
        result = aggregate_with_sharding(
            self.orderline_collection,
            group_key='idp',
            aggregate_functions=['SUM']
        )

        phases = result['cost'].get('phases', {})
        self.assertIn('map_time_ms', phases)
        self.assertIn('shuffle_time_ms', phases)
        self.assertIn('reduce_time_ms', phases)

        # Total time should be sum of phases
        total = phases['map_time_ms'] + phases['shuffle_time_ms'] + phases['reduce_time_ms']
        self.assertAlmostEqual(result['cost']['time_ms'], total, places=5)


if __name__ == '__main__':
    unittest.main()
