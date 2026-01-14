# Big Data Structure - NoSQL Storage Estimator

**ESILV A5 - Big Data Structure Course Project**

A comprehensive Python tool for analyzing NoSQL denormalization strategies, computing storage costs, sharding distribution, and query execution costs across different database schemas (DB1-DB5).

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Project Structure](#project-structure)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Demonstration Guide](#demonstration-guide)
   - [Part 1-2: Data Model & Sizing](#part-1-2-data-model-denormalization--sizing)
   - [Part 3: Filter & Join Operators](#part-3-filter--join-operators)
   - [Part 4: Aggregate Operators](#part-4-aggregate-operators)
   - [Part 5: Integration & DB Comparison](#part-5-integration--db-comparison)
6. [Running Tests](#running-tests)
7. [GUI Application](#gui-application)
8. [Key Concepts](#key-concepts)

---

## Project Overview

This project simulates the cost of NoSQL data models on the cloud, analyzing:

- **Storage costs** for different denormalization strategies (DB1-DB5)
- **Sharding distribution** across 1000 servers
- **Query execution costs** (time, carbon footprint, price)
- **Operator performance** (filter, join, aggregate with/without sharding)

### Database Schemas

| DB | Strategy | Description |
|----|----------|-------------|
| **DB1** | `Prod{[Cat],Supp}, St, Wa, OL, Cl` | Normalized - Product with embedded categories/supplier |
| **DB2** | `Prod{[Cat],Supp,[St]}, Wa, OL, Cl` | Product with embedded stocks |
| **DB3** | `St{Prod{[Cat],Supp}}, Wa, OL, Cl` | Stock-centric with embedded product |
| **DB4** | `St, Wa, OL{Prod{[Cat],Supp}}, Cl` | OrderLine with embedded product |
| **DB5** | `Prod{[Cat],Supp,[OL]}, St, Wa, Cl` | Product with embedded orderlines |

### Statistics Used

- 10^7 clients, 10^5 products, 4x10^9 order lines, 200 warehouses
- 1000 servers, 4M documents per server
- 5000 distinct brands, 50 Apple products

---

## Project Structure

```
Big-data-structure-project/
├── AppCore/
│   ├── bigdata/                    # Main package
│   │   ├── __init__.py            # Package exports
│   │   ├── models.py              # Collection class
│   │   ├── sizes.py               # Size computation
│   │   ├── sharding.py            # Sharding distribution
│   │   ├── query_costs.py         # Cost analyzer
│   │   ├── operators.py           # 6 DVL operators
│   │   ├── integration.py         # Q6/Q7 execution (Part 5)
│   │   └── queries.py             # Query executor
│   ├── schema/                     # JSON Schema files
│   │   ├── db1_product_schema.json
│   │   ├── db2_product_schema.json
│   │   ├── db3_stock_schema.json
│   │   ├── db4_orderline_schema.json
│   │   ├── db5_product_schema.json
│   │   └── ...
│   ├── stats.json                  # Statistics configuration
│   └── gui_app.py                  # Tkinter GUI
├── Scripts/
│   ├── compute_db_sizes.py         # Compare DB1-DB5 storage
│   ├── compute_sharding_stats.py   # Sharding analysis
│   ├── compute_query_costs.py      # Query cost analysis
│   ├── run_operators.py            # DVL operators demo
│   └── run_integration.py          # Q6/Q7 integration demo
├── Tests/
│   └── test_operators.py           # Unit tests
├── test_package.py                 # Integration tests
└── README.md                       # This file
```

---

## Installation

### Requirements

- Python 3.8+ (tested with Python 3.13)
- No external dependencies (uses Python standard library only)

### Setup

```bash
# Clone or download the project
cd Big-data-structure-project

# Verify Python version
python --version

# Test the package imports
python -c "from AppCore.bigdata import Collection, compute_sharding_distribution; print('OK')"
```

---

## Quick Start

```bash
# Run GUI application
python AppCore/gui_app.py

# Run all demos
python Scripts/compute_db_sizes.py
python Scripts/run_operators.py --format table
python Scripts/run_integration.py

# Run tests
python -m pytest Tests/test_operators.py -v
```

---

## Demonstration Guide

### Part 1-2: Data Model Denormalization & Sizing

**Objective**: Compute document/collection/database sizes for DB1-DB5

#### Step 1: View the statistics
```bash
# Windows
type AppCore\stats.json

# Linux/Mac
cat AppCore/stats.json
```

Expected output:
```json
{
    "nb_clients": 1000000,
    "nb_products": 100000,
    "nb_orderlines": 4000000000,
    "nb_warehouses": 200,
    "nb_servers": 1000,
    "distinct_brands": 5000
}
```

#### Step 2: Compute database sizes
```bash
python Scripts/compute_db_sizes.py
```

Expected output:
```
================================================================================
BIG DATA STRUCTURE - DATABASE SIZE COMPARISON
================================================================================

INDIVIDUAL COLLECTION SIZES
--------------------------------------------------------------------------------
Collection               Doc Size (B)    Doc Count        Total Size (GB)
--------------------------------------------------------------------------------
Product (DB1)                    1168       100,000               0.11 GB
Product (DB2)                    1336       100,000               0.12 GB
Stock (DB3)                      1484    20,000,000              27.65 GB
OrderLine (DB4)                  1484 4,000,000,000           5,529.60 GB
Product (DB5)                  259368       100,000              24.16 GB

DATABASE TOTALS (DB1-DB5)
--------------------------------------------------------------------------------
DB1: ~5926 GB (normalized)
DB4: ~5927 GB (OrderLine-centric)
DB5: ~25.9 GB (Product with embedded orderlines)
```

#### Step 3: Compute sharding distribution
```bash
python Scripts/compute_sharding_stats.py
```

Expected output:
```
SHARDING DISTRIBUTION ANALYSIS
================================================================================

Strategy: Stock sharded by #IDP
  Documents: 20,000,000
  Distinct values: 100,000
  Avg docs/server: 20,000.00
  Avg distinct values/server: 100.00

Strategy: OrderLine sharded by #IDC
  Documents: 4,000,000,000
  Distinct values: 1,000,000
  Avg docs/server: 4,000,000.00
  Avg distinct values/server: 1,000.00
```

---

### Part 3: Filter & Join Operators

**Objective**: Demonstrate filter and nested-loop operators with/without sharding

#### Step 1: Run operators demo
```bash
python Scripts/run_operators.py --format table
```

Expected output:
```
OPERATOR RESULTS
================================================================================

FILTER OPERATORS (product collection, brand filter)
--------------------------------------------------------------------------------
Operator                        Output Docs  Output (MB)    Time (ms)   Cost (USD)
--------------------------------------------------------------------------------
filter_with_sharding                     50        0.058        0.001     0.000000
filter_without_sharding                  50        0.058        2.000     0.000278

NESTED-LOOP JOIN OPERATORS (product x stock)
--------------------------------------------------------------------------------
Operator                              Docs Scanned     Output Docs    Time (ms)
--------------------------------------------------------------------------------
nested_loop_with_sharding                  5000000         5000000       100.00
nested_loop_without_sharding            1000000000         5000000     20000.00
```

**Key observation**: Sharded operators are significantly faster and cheaper.

#### Step 2: Run with JSON output
```bash
python Scripts/run_operators.py --format json
```

#### Step 3: Test with custom selectivity
```bash
python Scripts/run_operators.py --selectivity 0.1 --format table
```

---

### Part 4: Aggregate Operators

**Objective**: Demonstrate aggregate operators with Map/Reduce cost model

#### Step 1: Run Python interactive demo
```bash
python
```

```python
import json
from AppCore.bigdata import Collection, aggregate_with_sharding, aggregate_without_sharding

# Load stats
with open('AppCore/stats.json') as f:
    stats = json.load(f)

# Create OrderLine collection (4 billion documents)
orderline = Collection(
    name='orderline',
    schema={'properties': {'idp': {'type': 'integer'}, 'quantity': {'type': 'integer'}}},
    stats=stats,
    document_count=stats['nb_orderlines']
)

# Q6: Aggregate by product ID (SUM of quantities)
result = aggregate_with_sharding(
    orderline,
    group_key='idp',
    aggregate_functions=['SUM', 'COUNT']
)

print(f"Operator: {result['operator']}")
print(f"Documents scanned: {result['docs_scanned']:,}")
print(f"Number of groups: {result['num_groups']:,}")
print(f"Output documents: {result['output_docs']:,}")
print(f"Time: {result['cost']['time_ms']:.4f} ms")
print(f"Phases: {result['cost']['phases']}")
```

Expected output:
```
Operator: aggregate_with_sharding
Documents scanned: 4,000,000,000
Number of groups: 100,000
Output documents: 100,000
Time: 45.0100 ms
Phases: {'map_time_ms': 40.0, 'shuffle_time_ms': 5.0, 'reduce_time_ms': 0.01}
```

#### Step 2: Compare with/without sharding
```python
# With sharding (efficient)
with_shard = aggregate_with_sharding(orderline, 'idp', ['SUM'])
print(f"With sharding: {with_shard['cost']['time_ms']:.2f} ms")

# Without sharding (expensive)
without_shard = aggregate_without_sharding(orderline, 'idp', ['SUM'])
print(f"Without sharding: {without_shard['cost']['time_ms']:.2f} ms")

print(f"Speedup: {without_shard['cost']['time_ms'] / with_shard['cost']['time_ms']:.1f}x")
```

Expected output:
```
With sharding: 45.01 ms
Without sharding: 80105.00 ms
Speedup: 1780.0x
```

---

### Part 5: Integration & DB Comparison

**Objective**: Execute complex queries (Q6, Q7) across DB1-DB5 and compare costs

#### Step 1: Run integration demo
```bash
python Scripts/run_integration.py
```

Expected output:
```
Loaded statistics: 4,000,000,000 orderlines, 100,000 products
Loaded schemas: ['DB1', 'DB4', 'DB5']

======================================================================
QUERY Q6
======================================================================
Top 100 most ordered products (SUM of quantities)
SQL: SELECT P.name, P.price, SUM(O.quantity) FROM OrderLine O
     JOIN Product P ON O.IDP = P.IDP GROUP BY O.IDP
     ORDER BY SUM(O.quantity) DESC LIMIT 100

Query: Q6
----------------------------------------------------------------------
DB         Time (ms)       Carbon (kg)     Price ($)
----------------------------------------------------------------------
DB1        46.0100         0.00000251      0.0000126417
DB4        45.0100         0.00000248      0.0000125028
DB5        5.0110          0.00000028      0.0000013919
----------------------------------------------------------------------
Recommendation: Use DB5 (lowest cost)

======================================================================
QUERY Q7
======================================================================
Most ordered product by customer #125
SQL: SELECT P.name, P.price, SUM(O.quantity) FROM OrderLine O
     JOIN Product P ON O.IDP = P.IDP WHERE O.IDC = 125
     GROUP BY O.IDP ORDER BY SUM(O.quantity) DESC LIMIT 1

Query: Q7
----------------------------------------------------------------------
DB         Time (ms)       Carbon (kg)     Price ($)
----------------------------------------------------------------------
DB1        0.0002          0.00000000      0.0000000000
DB4        0.0002          0.00000000      0.0000000000
DB5        0.0002          0.00000000      0.0000000000
----------------------------------------------------------------------
Recommendation: Use DB5 (lowest cost)

======================================================================
SUMMARY - Best Database Recommendations
======================================================================
  Q6: Use DB5
  Q7: Use DB5
```

#### Step 2: Run with JSON output
```bash
python Scripts/run_integration.py --format json --query Q6
```

#### Step 3: Python API demo
```python
from AppCore.bigdata import IntegrationExecutor
import json

# Load schemas
schemas = {}
for db in ['db1_product', 'db4_orderline', 'db5_product']:
    with open(f'AppCore/schema/{db}_schema.json') as f:
        schemas[db.split('_')[0].upper()] = json.load(f)

with open('AppCore/stats.json') as f:
    stats = json.load(f)

# Create executor
executor = IntegrationExecutor(schemas, stats)

# Compare Q6 across all DBs
comparison = executor.compare_query_across_dbs('Q6')
print(f"Best DB for Q6: {comparison['recommendation']['best_db']}")

# Print comparison table
print(executor.print_comparison_table('Q6'))
```

---

## Running Tests

### Run all operator tests (15 tests)
```bash
python -m pytest Tests/test_operators.py -v
```

Expected output:
```
Tests/test_operators.py::TestFilterOperators::test_filter_with_sharding_basic PASSED
Tests/test_operators.py::TestFilterOperators::test_filter_without_sharding_basic PASSED
Tests/test_operators.py::TestFilterOperators::test_selectivity_provided PASSED
Tests/test_operators.py::TestFilterOperators::test_sharding_vs_no_sharding_cost_difference PASSED
Tests/test_operators.py::TestNestedLoopOperators::test_nested_loop_with_sharding_basic PASSED
Tests/test_operators.py::TestNestedLoopOperators::test_nested_loop_without_sharding_basic PASSED
Tests/test_operators.py::TestNestedLoopOperators::test_nested_loop_sharding_vs_no_sharding PASSED
Tests/test_operators.py::TestOutputSizeComputation::test_output_bytes_proportional_to_docs PASSED
Tests/test_operators.py::TestAggregateOperators::test_aggregate_with_sharding_basic PASSED
Tests/test_operators.py::TestAggregateOperators::test_aggregate_without_sharding_basic PASSED
Tests/test_operators.py::TestAggregateOperators::test_aggregate_num_groups_estimation PASSED
Tests/test_operators.py::TestAggregateOperators::test_aggregate_with_filter PASSED
Tests/test_operators.py::TestAggregateOperators::test_aggregate_sharding_vs_no_sharding_cost PASSED
Tests/test_operators.py::TestAggregateOperators::test_aggregate_output_size_small PASSED
Tests/test_operators.py::TestAggregateOperators::test_aggregate_phases_present PASSED

15 passed in 0.20s
```

### Run specific test class
```bash
# Test only aggregate operators
python -m pytest Tests/test_operators.py::TestAggregateOperators -v

# Test only filter operators
python -m pytest Tests/test_operators.py::TestFilterOperators -v
```

---

## GUI Application

### Launch the GUI
```bash
python AppCore/gui_app.py
```

Or on Windows, double-click `Launch_BigData_Tool.vbs`

### Features
- **Schema Selection**: Choose from available JSON schemas
- **Size Computation**: Calculate document/collection sizes
- **Sharding Analysis**: Analyze distribution across servers
- **Operators Panel**: Test filter, join, and aggregate operators
- **Cost Comparison**: Compare costs across different algorithms

---

## Key Concepts

### Sizing Rules (from course)
| Type | Size |
|------|------|
| Integer/Number | 8 bytes |
| String | 80 bytes |
| Date | 20 bytes |
| LongString | 200 bytes |
| Key-Value overhead | 12 bytes |
| Array elements | 2 (average) |

### Cost Model Constants
| Parameter | Value |
|-----------|-------|
| Docs per server | 4,000,000 |
| Server idle power | 200W |
| Server active power | 400W |
| Cloud cost | $0.50/hour/server |
| CO2 intensity | 0.233 kg/kWh |

### Operators Summary
| Operator | Algorithm | Use Case |
|----------|-----------|----------|
| `filter_with_sharding` | shard | Efficient filter on sharding key |
| `filter_without_sharding` | full_scan | Filter without index |
| `nested_loop_with_sharding` | nested_loop | Join with sharding optimization |
| `nested_loop_without_sharding` | nested_loop | Expensive join |
| `aggregate_with_sharding` | map_reduce | Distributed aggregation |
| `aggregate_without_sharding` | full_scan | Sequential aggregation |

### Query Definitions

**Q6**: Top 100 most ordered products
```sql
SELECT P.name, P.price, SUM(O.quantity) AS total
FROM OrderLine O JOIN Product P ON O.IDP = P.IDP
GROUP BY O.IDP
ORDER BY total DESC
LIMIT 100
```

**Q7**: Most ordered product by customer #125
```sql
SELECT P.name, P.price, SUM(O.quantity) AS total
FROM OrderLine O JOIN Product P ON O.IDP = P.IDP
WHERE O.IDC = 125
GROUP BY O.IDP
ORDER BY total DESC
LIMIT 1
```

---

## Summary of Implemented Parts

| Part | Chapter | Status | Key Files |
|------|---------|--------|-----------|
| 1-2 | Data Model Denormalization | Done | `sizes.py`, `sharding.py`, `models.py` |
| 3 | Filter & Join Queries | Done | `operators.py` (filter_*, nested_loop_*) |
| 4 | Aggregate Queries | Done | `operators.py` (aggregate_*) |
| 5 | Integration & Challenge | Done | `integration.py`, `run_integration.py` |

---

## Authors

BDS Project Team - ESILV A5

## License

Educational project for ESILV Big Data Structure course.
