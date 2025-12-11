# QUERY COST ANALYSIS - Section 3

## 3.0 - Query Cost Framework

### Cost Metrics

For each query, we estimate:

1. **Time** (milliseconds)
   - Based on documents scanned
   - Query algorithm efficiency
   - Network latency

2. **Carbon Footprint** (kg CO2)
   - Power consumption of servers accessed
   - Energy-based emissions (0.233 kg CO2/kWh)
   - Scales with: servers × duration

3. **Price** (USD)
   - Cloud provider costs (~$0.50/server/hour)
   - Scales with: servers × duration

### Query Algorithms

| Algorithm | Use Case | Speed | Carbon | Cost |
|-----------|----------|-------|--------|------|
| **Index** | Direct lookup (by indexed field) | [5/5] | Minimal | Minimal |
| **Shard** | Lookup using sharding key | [4/5] | Low | Low |
| **Nested Loop** | Filter on non-indexed field | [3/5] | Medium | Medium |
| **Full Scan** | Unfiltered table scan | [1/5] | High | High |

---

## 3.1 - Simple Filter Queries

### Q1: Stock of a given Product in a given Warehouse

**SQL Original:**
```sql
SELECT S.quantity, S.location
FROM Stock S
WHERE S.IDP = $IDP AND S.IDW = $IDW;
```

**MongoDB Query Language (MQL):**
```javascript
// DB1-DB2: Separate Stock collection
db.stock.findOne({
  "idp": 1234,
  "idw": 5
}, { "quantity": 1, "location": 1 })

// DB3: Stock with embedded Product
db.stock.findOne({
  "idp": 1234,
  "idw": 5
}, { "quantity": 1, "location": 1 })

// DB4-DB5: Still need Stock collection
db.stock.findOne({
  "idp": 1234,
  "idw": 5
}, { "quantity": 1, "location": 1 })
```

**Cost Analysis:**

| Database | Algorithm | Servers | Time (ms) | Carbon (kg) | Price (USD) | Notes |
|----------|-----------|---------|-----------|-------------|-------------|-------|
| **DB1** | Index on (IDP, IDW) | 1 | 0.001 | 0.000000 | 0.000000 | Perfect for direct lookup |
| **DB2** | Index on (IDP, IDW) | 1 | 0.001 | 0.000000 | 0.000000 | Same as DB1 |
| **DB3** | Index on (IDP, IDW) | 1 | 0.001 | 0.000000 | 0.000000 | Same structure |
| **DB4** | Index on (IDP, IDW) | 1 | 0.001 | 0.000000 | 0.000000 | Same structure |
| **DB5** | Need separate Stock | 1 | 0.001 | 0.000000 | 0.000000 | Stock still separate |

**Recommendation:** All DBs are equivalent for this query
- Composite index on (IDP, IDW) is essential
- Cost is negligible across all denormalizations
- **Best Practice:** Always index fields used in WHERE clauses

---

### Q2: Names and Prices of Products from a Brand

**SQL Original:**
```sql
SELECT P.name, P.price
FROM Product P
WHERE P.brand = $brand;
```

**MongoDB Query:**
```javascript
// All DBs (similar structure, Product is always a collection)
db.product.find({
  "brand": "Apple"
}, { "name": 1, "price": 1 })
```

**Cost Analysis:**

For 100,000 products with 5,000 distinct brands:
- Products per brand = 100,000 / 5,000 = **20 products** (on average, Apple has ~50)

| Database | Algorithm | Servers | Time (ms) | Carbon (kg) | Price (USD) | Notes |
|----------|-----------|---------|-----------|-------------|-------------|-------|
| **DB1-DB5** | Index on brand | 1 | 0.05 | 0.000000 | 0.000000 | Index lookup |
| **DB1-DB5** | Shard on brand | 5 | 0.1 | 0.000000 | 0.000000 | Via sharding key |
| **DB1-DB5** | Full scan (no index) | 1000 | 2.0 | 0.000052 | 0.000278 | Last resort |

**Recommendation:** Add index on `brand` field
- Index costs: ~0.05ms per query
- Full scan costs: ~2.0ms + 0.000278$ per query
- **Payback period:** 1-2 weeks of queries (worth it!)

---

### Q3: Product ID and Quantity from OrderLines at a Given Date

**SQL Original:**
```sql
SELECT O.IDP, O.quantity
FROM OrderLine O
WHERE O.date = $date;
```

**MongoDB Query:**
```javascript
// All DBs: OrderLine is always a separate collection
db.orderline.find({
  "date": ISODate("2024-12-11")
}, { "idp": 1, "quantity": 1 })
```

**Cost Analysis:**

For 4 billion OrderLines, assume ~10 million per day:

| Database | Algorithm | Servers | Time (ms) | Carbon (kg) | Price (USD) | Notes |
|----------|-----------|---------|-----------|-------------|-------------|-------|
| **DB1-DB5** | Index on date | 1 | 10 | 0.000000 | 0.000001 | Best case |
| **DB1-DB5** | Shard on date | 10 | 1 | 0.000000 | 0.000001 | Via sharding |
| **DB1-DB5** | Full scan (no index) | 1000 | 80 | 2.071370 | 11.111111 | Expensive! |

**Recommendation:** Index on `date` field is CRITICAL
- Indexed cost: 0.000001$ per query
- Full scan cost: 11.11$ per query
- **Savings:** 11 million times cheaper with index!
- With 1000 daily queries: saves ~$11,000/day

---

## 3.2 - Join Queries (with Filters)

### Q4: Stock Details from a Given Warehouse

**SQL Original:**
```sql
SELECT P.name, S.quantity
FROM Stock S
JOIN Product P ON S.IDP = P.IDP
WHERE S.IDW = $IDW;
```

**MongoDB Approaches by DB:**

#### DB1 (Normalized - requires join)
```javascript
// Requires lookup (simulates JOIN)
db.stock.aggregate([
  { $match: { "idw": 5 } },
  { $lookup: {
      from: "product",
      localField: "idp",
      foreignField: "idp",
      as: "product_details"
    }
  },
  { $project: {
      "product_details.name": 1,
      "quantity": 1
    }
  }
])
```

#### DB2-DB3 (Partial denormalization)
```javascript
// Stock with embedded Product info
db.stock.find({
  "idw": 5
}, { "product.name": 1, "quantity": 1 })
```

#### DB4-DB5 (Product-centric)
```javascript
// Must use Stock collection still
db.stock.find({
  "idw": 5
}, { "product.name": 1, "quantity": 1 })
```

**Cost Analysis:**

For warehouse with 100,000 products:

| Database | Approach | Algorithm | Servers | Time (ms) | Carbon (kg) | Price (USD) | Notes |
|----------|----------|-----------|---------|-----------|-------------|-------------|-------|
| **DB1** | JOIN | Nested loop | 2 | 100 | 0.000003 | 0.000014 | Requires $lookup |
| **DB2** | Embedded | Shard | 1 | 0.5 | 0.000000 | 0.000000 | Product embedded |
| **DB3** | Embedded | Shard | 1 | 0.5 | 0.000000 | 0.000000 | Product embedded |
| **DB4** | Separate | Nested loop | 2 | 100 | 0.000003 | 0.000014 | JOIN required |
| **DB5** | Separate | Nested loop | 2 | 100 | 0.000003 | 0.000014 | JOIN required |

**Recommendation:** DB2-DB3 WIN for this query
- DB2-DB3: 0.5ms (embedded Product)
- DB1,DB4,DB5: 100ms (requires JOIN)
- **Speed improvement:** 200x faster!
- **Cost improvement:** Same 200x cheaper

**Trade-off:** DB2-DB3 denormalize Product into Stock (duplication), but gains massive read performance.

---

### Q5: Distribution of Apple Products in Warehouses

**SQL Original:**
```sql
SELECT P.name, P.price, S.IDW, S.quantity
FROM Product P
JOIN Stock S ON P.IDP = S.IDP
WHERE P.brand = "Apple";
```

**MongoDB Approaches:**

#### DB1-DB2 (Product + Stock separate)
```javascript
db.product.aggregate([
  { $match: { "brand": "Apple" } },
  { $lookup: {
      from: "stock",
      localField: "idp",
      foreignField: "idp",
      as: "stocks"
    }
  },
  { $project: {
      "name": 1,
      "price": 1,
      "stocks.idw": 1,
      "stocks.quantity": 1
    }
  }
])
```

#### DB3 (Stock as root with embedded Product)
```javascript
db.stock.find({
  "product.brand": "Apple"
}, { "product.name": 1, "product.price": 1, "idw": 1, "quantity": 1 })
```

#### DB4-DB5 (OrderLine-centric)
```javascript
// Not ideal for this query - still need separate joins
db.product.aggregate([
  { $match: { "brand": "Apple" } },
  { $lookup: { from: "stock", ... } },
  ...
])
```

**Cost Analysis:**

For Apple brand (50 products, 200 warehouses = 10,000 stock entries):

| Database | Approach | Algorithm | Servers | Time (ms) | Carbon (kg) | Price (USD) | Notes |
|----------|----------|-----------|---------|-----------|-------------|-------------|-------|
| **DB1** | JOIN | Aggregation | 2 | 50 | 0.000001 | 0.000006 | Product → Stock |
| **DB2** | JOIN | Aggregation | 2 | 50 | 0.000001 | 0.000006 | Same as DB1 |
| **DB3** | Embedded | Shard on brand | 1 | 5 | 0.000000 | 0.000000 | Product embedded! |
| **DB4** | JOIN | Aggregation | 2 | 50 | 0.000001 | 0.000006 | Not optimized |
| **DB5** | JOIN | Aggregation | 2 | 50 | 0.000001 | 0.000006 | Not optimized |

**Recommendation:** DB3 WINS decisively
- DB3: 5ms (Stock with embedded Product)
- Others: 50ms (requires JOIN)
- **Speed improvement:** 10x faster!
- **Cost improvement:** 10x cheaper

**Why DB3 wins:** Stock as root with Product embedded makes warehouse queries ultra-fast.

---

## 3.3 - Summary: DB Comparison for All Queries

| Query | Type | DB1 | DB2 | DB3 | DB4 | DB5 | Winner |
|-------|------|-----|-----|-----|-----|-----|--------|
| **Q1** | Simple filter | Index | Index | Index | Index | Index | TIE ✓ |
| **Q2** | Simple filter | Index | Index | Index | Index | Index | TIE ✓ |
| **Q3** | Simple filter | Index | Index | Index | Index | Index | TIE ✓ |
| **Q4** | JOIN | 100ms | 0.5ms | 0.5ms | 100ms | 100ms | **DB2-DB3** ⭐⭐⭐ |
| **Q5** | JOIN | 50ms | 50ms | 5ms | 50ms | 50ms | **DB3** ⭐⭐⭐ |

---

## 3.4 - Conclusions & Recommendations

### Storage vs Performance Trade-off

| Database | Storage | Q1-Q3 | Q4 | Q5 | Overall |
|----------|---------|-------|----|----|---------|
| **DB1** | ✅ Minimal | ✓ | ❌ Slow | ❌ Slow | Normalization optimal for storage |
| **DB2** | ⚠️ +25GB | ✓ | ✅ Fast | ⚠️ Medium | Good middle ground |
| **DB3** | ⚠️ +25GB | ✓ | ✅ Fast | ✅✅ Fastest | **BEST for warehouse queries** |
| **DB4** | ❌ +5900GB | ✓ | ❌ Slow | ❌ Slow | Not recommended |
| **DB5** | ✅ Minimal | ✓ | ❌ Slow | ❌ Slow | Good for order history queries |

### Final Recommendation

**For this e-commerce application:**

1. **If read-heavy (typical case):** Use **DB3**
   - Stock as root with embedded Product
   - Excellent for warehouse queries (Q4-Q5)
   - Minimal storage overhead (~25GB)
   - Simple queries (Q1-Q3) stay indexed

2. **If write-heavy or bulk operations:** Use **DB1**
   - Normalized structure
   - Minimal storage (5926GB for full dataset)
   - More complex queries but maintainable

3. **If balanced (hybrid):** Use **DB2**
   - Product with stocks embedded
   - Good performance for most queries
   - Moderate storage

### Key Implementation Tips

```javascript
// 1. ALWAYS ADD INDEXES
db.product.createIndex({ "brand": 1 });
db.stock.createIndex({ "idp": 1, "idw": 1 });
db.orderline.createIndex({ "date": 1 });

// 2. USE DENORMALIZATION STRATEGICALLY
// Embed data that's frequently accessed together

// 3. SHARD ON HIGH-CARDINALITY KEYS
// db.stock.shardCollection("db.stock", { "idp": 1 })

// 4. PROFILE QUERIES
// Use MongoDB explain() to verify index usage
db.stock.find({...}).explain("executionStats")

// 5. MONITOR PERFORMANCE
// Track slow queries and adjust indexes accordingly
```

---

## Cost Savings Summary

With proper indexes and denormalization:

| Metric | Without Optimization | With Optimization | Savings |
|--------|----------------------|-------------------|---------|
| **Average Query Time** | 50ms | 1ms | 50x faster |
| **Daily Query Cost** | $11,111 | $22 | $11,089/day |
| **Yearly Cost** | $4.05M | $8,000 | $4.04M/year |
| **Carbon Footprint** | 2071kg CO2/day | 4kg CO2/day | 518x cleaner |

**Optimization is critical for scale!**