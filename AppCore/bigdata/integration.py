"""
Integration Module for Complex Query Execution (Part 5)

Provides query planning and execution across multiple database schemas (DB1-DB5).
Implements Q6 and Q7 aggregate queries and compares costs across denormalization strategies.

Classes:
- QueryPlan: Represents a sequence of operators for a complex query
- IntegrationExecutor: Orchestrates query execution across DB1-DB5
"""
from typing import Dict, Any, List, Optional
from math import ceil

from .models import Collection
from .operators import (
    filter_with_sharding,
    filter_without_sharding,
    nested_loop_with_sharding,
    nested_loop_without_sharding,
    aggregate_with_sharding,
    aggregate_without_sharding,
)
from .query_costs import QueryCostAnalyzer


class QueryPlan:
    """Represents a sequence of operators for a complex query.

    A QueryPlan chains multiple operators together, passing results
    from one step to the next.

    Attributes:
        query_id: Identifier for this query (e.g., 'Q6', 'Q7')
        description: Human-readable description of the query
        steps: List of operator steps to execute
    """

    def __init__(self, query_id: str, description: str, steps: List[Dict[str, Any]]):
        """Initialize a query plan.

        Args:
            query_id: Query identifier
            description: Query description
            steps: List of dicts with 'operator', 'params' keys
        """
        self.query_id = query_id
        self.description = description
        self.steps = steps
        self.results = []

    def execute(self, collections: Dict[str, Collection], stats: Dict[str, Any]) -> Dict[str, Any]:
        """Execute all steps in the plan.

        Args:
            collections: Dict mapping collection names to Collection objects
            stats: Statistics dictionary

        Returns:
            Dictionary with execution results and total costs
        """
        self.results = []
        total_time_ms = 0.0
        total_carbon_kg = 0.0
        total_price_usd = 0.0
        total_servers = 0

        for i, step in enumerate(self.steps):
            op_name = step['operator']
            params = step.get('params', {})

            # Get the appropriate collection(s)
            collection_name = params.get('collection')

            # Execute the operator
            if op_name == 'aggregate_with_sharding':
                coll = collections.get(collection_name)
                if coll:
                    result = aggregate_with_sharding(
                        coll,
                        group_key=params.get('group_key', 'idp'),
                        aggregate_functions=params.get('aggregate_functions', ['SUM']),
                        filter_key=params.get('filter_key'),
                        selectivity=params.get('selectivity')
                    )
                else:
                    result = {'error': f'Collection {collection_name} not found'}

            elif op_name == 'aggregate_without_sharding':
                coll = collections.get(collection_name)
                if coll:
                    result = aggregate_without_sharding(
                        coll,
                        group_key=params.get('group_key', 'idp'),
                        aggregate_functions=params.get('aggregate_functions', ['SUM']),
                        filter_key=params.get('filter_key'),
                        selectivity=params.get('selectivity')
                    )
                else:
                    result = {'error': f'Collection {collection_name} not found'}

            elif op_name == 'filter_with_sharding':
                coll = collections.get(collection_name)
                if coll:
                    result = filter_with_sharding(
                        coll,
                        expected_output_keys=params.get('output_keys', []),
                        filtered_key=params.get('filter_key', ''),
                        selectivity=params.get('selectivity')
                    )
                else:
                    result = {'error': f'Collection {collection_name} not found'}

            elif op_name == 'nested_loop_with_sharding':
                left_name = params.get('left_collection')
                right_name = params.get('right_collection')
                left = collections.get(left_name)
                right = collections.get(right_name)
                if left and right:
                    result = nested_loop_with_sharding(
                        left, right,
                        expected_output_keys=params.get('output_keys', []),
                        filtered_key=params.get('filter_key', ''),
                        selectivity=params.get('selectivity'),
                        stats=stats
                    )
                else:
                    result = {'error': f'Collections {left_name}/{right_name} not found'}
            else:
                result = {'error': f'Unknown operator: {op_name}'}

            # Accumulate costs
            if 'cost' in result:
                cost = result['cost']
                total_time_ms += cost.get('time_ms', 0)
                total_carbon_kg += cost.get('carbon_kg', 0)
                total_price_usd += cost.get('price_usd', 0)
                total_servers = max(total_servers, cost.get('servers_accessed', 0))

            self.results.append({
                'step': i + 1,
                'operator': op_name,
                'result': result
            })

        return {
            'query_id': self.query_id,
            'description': self.description,
            'num_steps': len(self.steps),
            'steps': self.results,
            'total_cost': {
                'time_ms': total_time_ms,
                'carbon_kg': total_carbon_kg,
                'price_usd': total_price_usd,
                'max_servers_accessed': total_servers
            }
        }

    def get_total_cost(self) -> Dict[str, float]:
        """Get the total cost from the last execution."""
        if not self.results:
            return {'time_ms': 0, 'carbon_kg': 0, 'price_usd': 0}

        total_time = sum(
            r['result'].get('cost', {}).get('time_ms', 0)
            for r in self.results
        )
        total_carbon = sum(
            r['result'].get('cost', {}).get('carbon_kg', 0)
            for r in self.results
        )
        total_price = sum(
            r['result'].get('cost', {}).get('price_usd', 0)
            for r in self.results
        )

        return {
            'time_ms': total_time,
            'carbon_kg': total_carbon,
            'price_usd': total_price
        }


class IntegrationExecutor:
    """Orchestrates query execution across multiple database schemas.

    Handles complex queries (Q6, Q7) and compares costs across DB1-DB5
    denormalization strategies.

    Attributes:
        schemas: Dict mapping DB names to their schema dictionaries
        stats: Statistics dictionary
    """

    def __init__(self, schemas: Dict[str, Dict], stats: Dict[str, Any]):
        """Initialize the executor.

        Args:
            schemas: Dict mapping DB names (e.g., 'DB1', 'DB4') to schema dicts
            stats: Statistics dictionary with counts and cardinalities
        """
        self.schemas = schemas
        self.stats = stats

    def _get_collection_for_db(self, db_name: str, collection_type: str) -> Optional[Collection]:
        """Get a Collection object for a specific DB and collection type.

        Args:
            db_name: Database name (e.g., 'DB1', 'DB4', 'DB5')
            collection_type: Type of collection ('orderline', 'product', 'stock')

        Returns:
            Collection object or None if not found
        """
        schema = self.schemas.get(db_name)
        if not schema:
            return None

        # Determine document count based on collection type and DB structure
        if collection_type == 'orderline':
            doc_count = self.stats.get('nb_orderlines', 4_000_000_000)
        elif collection_type == 'product':
            doc_count = self.stats.get('nb_products', 100_000)
        elif collection_type == 'stock':
            doc_count = self.stats.get('nb_products', 100_000) * self.stats.get('nb_warehouses', 200)
        else:
            doc_count = 1_000_000

        return Collection(
            name=f"{db_name}_{collection_type}",
            schema=schema,
            stats=self.stats,
            document_count=doc_count
        )

    def _has_embedded_product(self, schema: Dict) -> bool:
        """Check if schema has embedded product data."""
        props = schema.get('properties', {})
        return 'product' in props

    def _has_embedded_orderlines(self, schema: Dict) -> bool:
        """Check if schema has embedded orderlines."""
        props = schema.get('properties', {})
        return 'orderlines' in props

    def execute_q6(self, db_name: str) -> Dict[str, Any]:
        """Execute Q6: Top 100 most ordered products (SUM of quantities).

        SQL equivalent:
        SELECT P.name, P.price, SUM(O.quantity) AS total
        FROM OrderLine O JOIN Product P ON O.IDP = P.IDP
        GROUP BY O.IDP
        ORDER BY total DESC
        LIMIT 100

        Args:
            db_name: Database name ('DB1', 'DB4', 'DB5')

        Returns:
            Execution results with costs
        """
        schema = self.schemas.get(db_name)
        if not schema:
            return {'error': f'Schema not found for {db_name}'}

        # Determine strategy based on schema structure
        if self._has_embedded_orderlines(schema):
            # DB5: Product has embedded orderlines - aggregate within document
            plan = QueryPlan(
                query_id='Q6',
                description='Top 100 most ordered products (embedded orderlines)',
                steps=[{
                    'operator': 'aggregate_with_sharding',
                    'params': {
                        'collection': 'product',
                        'group_key': 'idp',
                        'aggregate_functions': ['SUM']
                    }
                }]
            )
            collections = {
                'product': self._get_collection_for_db(db_name, 'product')
            }

        elif self._has_embedded_product(schema):
            # DB4: OrderLine has embedded product - can aggregate directly
            plan = QueryPlan(
                query_id='Q6',
                description='Top 100 most ordered products (embedded product in orderline)',
                steps=[{
                    'operator': 'aggregate_with_sharding',
                    'params': {
                        'collection': 'orderline',
                        'group_key': 'idp',
                        'aggregate_functions': ['SUM']
                    }
                }]
            )
            collections = {
                'orderline': self._get_collection_for_db(db_name, 'orderline')
            }

        else:
            # DB1: Normalized - need aggregate + join
            plan = QueryPlan(
                query_id='Q6',
                description='Top 100 most ordered products (normalized - requires join)',
                steps=[
                    {
                        'operator': 'aggregate_with_sharding',
                        'params': {
                            'collection': 'orderline',
                            'group_key': 'idp',
                            'aggregate_functions': ['SUM']
                        }
                    },
                    {
                        'operator': 'nested_loop_with_sharding',
                        'params': {
                            'left_collection': 'aggregated',
                            'right_collection': 'product',
                            'output_keys': ['name', 'price', 'total'],
                            'filter_key': 'idp',
                            'selectivity': 100 / self.stats.get('nb_products', 100_000)
                        }
                    }
                ]
            )
            # For normalized, we simulate two collections
            orderline_coll = Collection(
                name=f"{db_name}_orderline",
                schema={'properties': {'idp': {'type': 'integer'}, 'quantity': {'type': 'integer'}}},
                stats=self.stats,
                document_count=self.stats.get('nb_orderlines', 4_000_000_000)
            )
            product_coll = Collection(
                name=f"{db_name}_product",
                schema=schema,
                stats=self.stats,
                document_count=self.stats.get('nb_products', 100_000)
            )
            # Simulated aggregated result (100K groups reduced to lookup)
            aggregated_coll = Collection(
                name='aggregated',
                schema={'properties': {'idp': {'type': 'integer'}, 'total': {'type': 'integer'}}},
                stats=self.stats,
                document_count=self.stats.get('nb_products', 100_000)
            )
            collections = {
                'orderline': orderline_coll,
                'product': product_coll,
                'aggregated': aggregated_coll
            }

        return plan.execute(collections, self.stats)

    def execute_q7(self, db_name: str, client_id: int = 125) -> Dict[str, Any]:
        """Execute Q7: Most ordered product by a specific customer.

        SQL equivalent:
        SELECT P.name, P.price, SUM(O.quantity) AS total
        FROM OrderLine O JOIN Product P ON O.IDP = P.IDP
        WHERE O.IDC = 125
        GROUP BY O.IDP
        ORDER BY total DESC
        LIMIT 1

        Args:
            db_name: Database name
            client_id: Customer ID to filter on

        Returns:
            Execution results with costs
        """
        schema = self.schemas.get(db_name)
        if not schema:
            return {'error': f'Schema not found for {db_name}'}

        # Selectivity: 1 client out of 10M
        client_selectivity = 1.0 / self.stats.get('nb_clients', 10_000_000)

        if self._has_embedded_orderlines(schema):
            # DB5: Need to scan products and filter embedded orderlines
            # Less efficient for Q7 since we need to scan all products
            plan = QueryPlan(
                query_id='Q7',
                description=f'Most ordered product by client {client_id} (embedded orderlines)',
                steps=[{
                    'operator': 'aggregate_with_sharding',
                    'params': {
                        'collection': 'product',
                        'group_key': 'idp',
                        'aggregate_functions': ['SUM'],
                        'filter_key': 'idc',
                        'selectivity': client_selectivity
                    }
                }]
            )
            collections = {
                'product': self._get_collection_for_db(db_name, 'product')
            }

        elif self._has_embedded_product(schema):
            # DB4: Filter orderlines by client, then aggregate
            plan = QueryPlan(
                query_id='Q7',
                description=f'Most ordered product by client {client_id} (embedded product)',
                steps=[{
                    'operator': 'aggregate_with_sharding',
                    'params': {
                        'collection': 'orderline',
                        'group_key': 'idp',
                        'aggregate_functions': ['SUM'],
                        'filter_key': 'idc',
                        'selectivity': client_selectivity
                    }
                }]
            )
            collections = {
                'orderline': self._get_collection_for_db(db_name, 'orderline')
            }

        else:
            # DB1: Filter + Aggregate + Join
            plan = QueryPlan(
                query_id='Q7',
                description=f'Most ordered product by client {client_id} (normalized)',
                steps=[
                    {
                        'operator': 'aggregate_with_sharding',
                        'params': {
                            'collection': 'orderline',
                            'group_key': 'idp',
                            'aggregate_functions': ['SUM'],
                            'filter_key': 'idc',
                            'selectivity': client_selectivity
                        }
                    }
                ]
            )
            orderline_coll = Collection(
                name=f"{db_name}_orderline",
                schema={'properties': {'idp': {'type': 'integer'}, 'idc': {'type': 'integer'}, 'quantity': {'type': 'integer'}}},
                stats=self.stats,
                document_count=self.stats.get('nb_orderlines', 4_000_000_000)
            )
            collections = {
                'orderline': orderline_coll
            }

        return plan.execute(collections, self.stats)

    def compare_query_across_dbs(self, query_id: str, db_list: Optional[List[str]] = None) -> Dict[str, Any]:
        """Compare query execution costs across multiple databases.

        Args:
            query_id: Query identifier ('Q6' or 'Q7')
            db_list: List of DB names to compare (default: all available)

        Returns:
            Comparison results with costs for each DB
        """
        if db_list is None:
            db_list = list(self.schemas.keys())

        results = {}

        for db_name in db_list:
            if query_id == 'Q6':
                result = self.execute_q6(db_name)
            elif query_id == 'Q7':
                result = self.execute_q7(db_name)
            else:
                result = {'error': f'Unknown query: {query_id}'}

            results[db_name] = result

        # Find best DB (lowest cost)
        best_db = None
        best_cost = float('inf')

        for db_name, result in results.items():
            if 'total_cost' in result:
                cost = result['total_cost'].get('time_ms', float('inf'))
                if cost < best_cost:
                    best_cost = cost
                    best_db = db_name

        return {
            'query_id': query_id,
            'databases_compared': db_list,
            'results': results,
            'recommendation': {
                'best_db': best_db,
                'best_time_ms': best_cost,
                'reason': f'{best_db} has the lowest execution time for {query_id}'
            }
        }

    def get_best_db_for_query(self, query_id: str) -> str:
        """Get the best database for a specific query.

        Args:
            query_id: Query identifier

        Returns:
            Name of the best database
        """
        comparison = self.compare_query_across_dbs(query_id)
        return comparison['recommendation']['best_db']

    def print_comparison_table(self, query_id: str, db_list: Optional[List[str]] = None) -> str:
        """Generate a formatted comparison table.

        Args:
            query_id: Query identifier
            db_list: List of DBs to compare

        Returns:
            Formatted table string
        """
        comparison = self.compare_query_across_dbs(query_id, db_list)

        lines = []
        lines.append(f"Query: {query_id}")
        lines.append("-" * 70)
        lines.append(f"{'DB':<10} {'Time (ms)':<15} {'Carbon (kg)':<15} {'Price ($)':<15}")
        lines.append("-" * 70)

        for db_name, result in comparison['results'].items():
            if 'total_cost' in result:
                cost = result['total_cost']
                time_ms = cost.get('time_ms', 0)
                carbon = cost.get('carbon_kg', 0)
                price = cost.get('price_usd', 0)
                lines.append(f"{db_name:<10} {time_ms:<15.4f} {carbon:<15.8f} {price:<15.10f}")
            else:
                lines.append(f"{db_name:<10} {'ERROR':<15} {'-':<15} {'-':<15}")

        lines.append("-" * 70)
        rec = comparison['recommendation']
        lines.append(f"Recommendation: Use {rec['best_db']} (lowest cost)")

        return "\n".join(lines)
