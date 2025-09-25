# Iceberg Equality Delete Marker Architecture

## Overview

This document describes the architecture of **Iceberg Equality Delete Marker** optimization in StarRocks - a feature that enables efficient row group bypass during equality delete operations by analyzing row group statistics instead of performing expensive hash join probes.

## Problem Statement

### Traditional Iceberg Equality Delete Flow
```
1. Scan data table → produces rows
2. Hash Join (LEFT_ANTI_JOIN) with delete table → filters out deleted rows
3. Return non-deleted rows
```

**Performance Issue:** Hash join probe is expensive for every row, even when entire row groups contain no deleted data.

### Solution: Row Group Bypass Optimization
```
1. Analyze row group statistics (min/max, bloom filters) against delete values
2. If row group guaranteed NOT to contain deleted data → BYPASS hash join
3. Mark these rows with bypass flag → skip expensive hash probe
4. Apply hash join only to remaining rows
```

## Key Architecture Decisions

### 1. Dual Delivery Mechanism

| **Mechanism** | **Purpose** | **Level** | **Action** |
|---|---|---|---|
| **Standard RF** | Row-level filtering | Row | `chunk->filter()` - REMOVES rows |
| **EQ-delete RF** | Row group bypass | Row group | `chunk->append_column(bypass)` - MARKS rows |

### 2. Why Bypass Standard RuntimeFilterProbeCollector

**Standard Flow:**
```cpp
RuntimeFilterProbeCollector::evaluate(chunk) {
    filter->evaluate(column, &context);  // ❌ FILTERS ROWS - removes deleted rows
    chunk->filter(selection);            // ❌ WE NEED THESE ROWS for bypass marking!
}
```

**⚠️ CRITICAL ISSUE: Runtime Filter → Conjuncts Conversion**

Standard runtime filters are automatically converted to conjuncts in **TWO PLACES**, both incompatible with LEFT_ANTI_JOIN semantics for Iceberg equality deletes:

**1. Min-Max Conjuncts Conversion** in `DataSource::parse_runtime_filters()` (be/src/connector/connector.cpp:93-99):
```cpp
// ❌ PROBLEM: Creates min-max predicate from ANY runtime filter
RuntimeFilterHelper::create_min_max_value_predicate(..., filter, &min_max_predicate);
// ❌ ADDS TO CONJUNCTS - will filter rows incorrectly for anti-join
_conjunct_ctxs.insert(_conjunct_ctxs.begin(), ctx);
```

**2. IN Filter Conjuncts Conversion** via `Operator::set_precondition_ready()` → `runtime_in_filters`:
- **Source:** `be/src/exec/pipeline/operator.cpp:104-108`
- **OLAP Scans:** `be/src/exec/pipeline/scan/olap_scan_context.cpp:113`
- **Connector Scans:** `be/src/exec/pipeline/scan/connector_scan_operator.cpp:595`

```cpp
// ❌ IN filters become conjuncts in ALL scan types
_conjunct_ctxs.insert(_conjunct_ctxs.end(), runtime_in_filters.begin(), runtime_in_filters.end());
```

**Why this breaks EQ-delete:**

EQ-delete optimization requires **ALL rows from data files** to reach HashJoiner for proper anti-join processing. We should only **MARK rows for bypass** (skip hash probe), not **FILTER them out**.

- **InRuntimeFilter** contains DELETED values: `[5, 10, 15]`
- **MinMax conjunct** becomes: `column >= 5 AND column <= 15` → **FILTERS OUT** rows `{1,2,3,4,16,17,...}`
- **IN conjunct** becomes: `column IN (5, 10, 15)` → **FILTERS OUT** rows `{1,2,3,4,6,7,8,9,11,12,13,14,16,17,...}`
- **Problem:** Anti-join **NEEDS ALL ROWS** `{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,...}` to return correct result
- **Conjuncts restrict scan** → **Missing rows in anti-join result** → **INCORRECT QUERY RESULT**

**Correct EQ-delete behavior:**
- **Scan ALL rows** from data files
- **Mark bypass-eligible rows** with `EQ_DELETE_BYPASS_SLOT_ID` column
- **HashJoiner processes ALL rows** but skips hash probe for marked rows

**EQ-delete Flow:**
```cpp
FileReader::_should_bypass_eq_delete_for_row_group() {
    // Evaluate predicates against ROW GROUP metadata (not row data)
    // Return bypass decision for entire row group
}
```

## Component Architecture

### Frontend Components

#### Core Planner Changes

**File:** `fe/fe-core/src/main/java/com/starrocks/qe/SessionVariable.java`
- **NEW:** Added `ENABLE_ICEBERG_EQUALITY_DELETE_OPTIMIZATION` session variable (default: true)

**File:** `fe/fe-core/src/main/java/com/starrocks/planner/HashJoinNode.java`
- **NEW:** Added `isIcebergEqualityDelete` flag for marking EQ-delete anti-joins
- **NEW:** Serializes flag to `THashJoinNode.is_iceberg_equality_delete`

**File:** `fe/fe-core/src/main/java/com/starrocks/planner/JoinNode.java`
- **CRITICAL:** Modified runtime filter creation logic to allow LEFT_ANTI_JOIN for Iceberg EQ-delete
- **CRITICAL:** Special pushdown logic - EQ-delete RFs ALWAYS pushed to scan nodes, never kept at join level

**File:** `fe/fe-core/src/main/java/com/starrocks/planner/RuntimeFilterDescription.java`
- **NEW:** Added `isIcebergEqualityDelete` flag
- Sets `is_iceberg_equality_delete = true` in `TRuntimeFilterDescription`
- Builds `plan_node_id_to_target_expr` mapping (scan_node_id → probe_expr)

#### Optimizer Integration

**File:** `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalIcebergScanOperator.java`
- **NEW:** Added `isEqDeleteProbeScan` flag for marking probe scan operators

**File:** `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalJoinOperator.java`
- **NEW:** Added `isIcebergEqualityDelete` flag for logical join operators

**File:** `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/IcebergEqualityDeleteRewriteRule.java`
- **MODIFIED:** Sets `isIcebergEqualityDelete(true)` on created anti-join operators
- **MODIFIED:** Sets `isEqDeleteProbeScan(true)` on probe scan operators

**File:** `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/implementation/HashJoinImplementationRule.java`
- **MODIFIED:** Passes through `isIcebergEqualityDelete` flag from logical to physical operators

### Backend Components Flow

```
1. TRuntimeFilterDescription (Thrift)
   ├── filter_id: 123
   ├── is_iceberg_equality_delete: true
   └── plan_node_id_to_target_expr: {scan_node_id → probe_expr}

2. RuntimeFilterBuildDescriptor (HashJoiner)
   ├── Stores plan_node_id_to_target_expr map
   ├── Provides get_plan_node_id_to_target_expr() access
   └── is_iceberg_eq_delete_filter() flag

3. InRuntimeFilter (HashJoiner)
   ├── type() = EQ_DELETE_MARKER
   ├── Contains hash set of deleted values

4. RuntimeFilterProbeDescriptor (Direct delivery)
   ├── Encapsulates InRuntimeFilter + column metadata
   ├── probe_expr_ctx() → column info (SlotId, LogicalType)
   └── runtime_filter() → InRuntimeFilter

5. RuntimeState (Cross-component delivery)
   ├── Key: scan_node_id
   └── Value: vector<RuntimeFilterProbeDescriptor*>

6. HiveConnector (Bridge)
   ├── Extracts markers by scan_node_id
   └── Passes to scanner_params.eq_delete_markers

7. FileReader (Row group analysis)
   ├── Converts InRuntimeFilter → ColumnPredicate → PredicateTree
   ├── Evaluates against row group statistics
   └── Makes bypass decisions per row group
```

## Detailed Component Analysis

### HashJoiner::_create_in_runtime_filters_for_eq_delete()

**Responsibility:** Create and deliver EQ-delete markers

```cpp
for (auto* rf_desc : _build_runtime_filters) {
    if (!rf_desc->is_iceberg_eq_delete_filter()) continue;  // Only EQ-delete

    // 1. Create InRuntimeFilter
    RuntimeFilter* in_rf = RuntimeFilterHelper::create_eq_delete_runtime_filter(...);

    // 2. Fill with hash table values
    RuntimeFilterHelper::fill_runtime_filter(hash_table_column, build_type, in_rf);

    // 3. Create RuntimeFilterProbeDescriptor with metadata
    auto* probe_descriptor = _pool->add(new RuntimeFilterProbeDescriptor());
    probe_descriptor->init(filter_id, probe_expr);
    probe_descriptor->set_runtime_filter(in_rf);

    // 4. Deliver to target scan nodes via RuntimeState (bypass standard ProbeCollector)
    const auto& target_nodes = rf_desc->get_plan_node_id_to_target_expr();
    for (const auto& [scan_node_id, target_expr] : target_nodes) {
        state->set_eq_delete_markers(scan_node_id, {probe_descriptor});
    }
}
```

**Key Point:** Uses fragment ObjectPool (_pool) since probe side is in same fragment.

### FileReader::_init_eq_delete_predicates()

**Responsibility:** Convert InRuntimeFilter to predicate evaluation infrastructure

```cpp
void FileReader::_init_eq_delete_predicates() {
    for (auto* probe_descriptor : _scanner_ctx->eq_delete_markers) {
        // 1. Extract InRuntimeFilter and column metadata
        auto* rf = probe_descriptor->runtime_filter(0);
        auto* in_filter = rf->get_in_filter();
        SlotId slot_id = probe_descriptor->probe_expr_ctx()->root()->slot_id();
        LogicalType slot_type = probe_descriptor->probe_expr_ctx()->root()->type().type;

        // 2. REUSE existing RuntimeColumnPredicateBuilder infrastructure
        auto predicates_result = type_dispatch_predicate<StatusOr<std::vector<const ColumnPredicate*>>>(
            slot_type, false, detail::RuntimeColumnPredicateBuilder(),
            _scanner_ctx->global_dictmaps, &parser,
            probe_descriptor, slot, /*driver_sequence=*/0, &_eq_delete_marker_pool);

        // 3. Build PredicateTree once for all row group evaluations
        _eq_delete_predicate_tree = std::make_unique<PredicateTree>(PredicateTree::create(pred_tree));
    }
}
```

**Key Points:**
- **Reuses existing infrastructure:** `RuntimeColumnPredicateBuilder`, `PredicateTree`, `PredicateFilterEvaluator`
- **One-time initialization:** Predicate parsing happens once in init, not in hot path
- **Memory management:** Uses `_eq_delete_marker_pool` ObjectPool for ColumnPredicate lifetime

### FileReader Row Group Processing

**Responsibility:** Static analysis and bypass decision caching

```cpp
// In _init_group_readers() - static analysis:
for (each row_group in file) {
    if (_filter_group(row_group)) continue;  // Standard conjunct filtering

    // EQ-delete bypass decision (static analysis)
    bool bypass_decision = _should_bypass_eq_delete_for_row_group(row_group_reader);
    _row_group_bypass_decisions.push_back(bypass_decision);

    _row_group_readers.emplace_back(row_group_reader);
}

// In get_next() - use cached decision:
bool should_bypass_eq_delete = _row_group_bypass_decisions[_cur_row_group_idx];
if (should_bypass_eq_delete) {
    auto bypass_column = BooleanColumn::create(row_count, 1);
    chunk->append_column(bypass_column, Chunk::EQ_DELETE_BYPASS_SLOT_ID);
}
```

**Key Points:**
- **Static analysis:** Bypass decisions computed once per row group in init
- **Cached decisions:** No recomputation in get_next() hot path
- **Correct timing:** After standard RF checks (_update_rf_and_filter_group)

### FileReader::_should_bypass_eq_delete_for_row_group()

**Responsibility:** Predicate evaluation against row group statistics

```cpp
bool FileReader::_should_bypass_eq_delete_for_row_group(const GroupReaderPtr& group_reader) {
    if (_eq_delete_predicate_tree == nullptr) return false;

    // Use existing PredicateFilterEvaluator infrastructure
    auto visitor = PredicateFilterEvaluator{*_eq_delete_predicate_tree, group_reader.get(),
                                            _scanner_ctx->parquet_page_index_enable,
                                            _scanner_ctx->parquet_bloom_filter_enable};
    auto res = _eq_delete_predicate_tree->visit(visitor);

    // If span_size() == 0 → row group does NOT contain deleted values → bypass!
    return (res.ok() && res.value().has_value() && res.value()->span_size() == 0);
}
```

**Key Points:**
- **Reuses existing evaluator:** `PredicateFilterEvaluator` for row group statistics
- **Standard logic:** Same evaluation as conjunct filtering
- **Correct semantics:** Empty span = no matching rows = safe to bypass

## Data Flow Diagram

```
Frontend:
├── IcebergEqualityDeleteRewriteRule.java
├── RuntimeFilterDescription.java
└── TRuntimeFilterDescription{is_iceberg_equality_delete: true}
                    ↓
Backend - HashJoiner:
├── RuntimeFilterBuildDescriptor.is_iceberg_eq_delete_filter() = true
├── create InRuntimeFilter{type: EQ_DELETE_MARKER}
├── create RuntimeFilterProbeDescriptor{filter + metadata}
└── RuntimeState[scan_node_id] = {probe_descriptor}
                    ↓
Backend - HiveConnector:
├── get RuntimeFilterProbeDescriptor by scan_node_id
└── scanner_params.eq_delete_markers = {probe_descriptor}
                    ↓
Backend - FileReader:
├── Convert to PredicateTree (once in init)
├── Evaluate against row group stats (cached per row group)
└── Mark bypass chunks with EQ_DELETE_BYPASS_SLOT_ID column
```

## Key Files and Changes

### Core Components
- **`be/src/exec/hash_joiner.cpp`:** EQ-delete marker creation and delivery
- **`be/src/exprs/runtime_filter_bank.{h,cpp}`:** Enhanced RuntimeFilterBuildDescriptor
- **`be/src/exprs/agg_in_runtime_filter.h`:** EQ_DELETE_MARKER type and debug names
- **`be/src/runtime/runtime_state.h`:** Direct delivery storage
- **`be/src/connector/hive_connector.cpp`:** Bridge component
- **`be/src/exec/hdfs_scanner.h`:** Data structures
- **`be/src/formats/parquet/file_reader.{h,cpp}`:** Row group analysis and bypass

## Performance Benefits

### Row Group Level Optimization
- **Before:** Hash join probe for every row (O(rows))
- **After:** Predicate evaluation per row group (O(row_groups))

### Memory Management
- **ObjectPool lifecycle:** Uses fragment-level pool for proper lifetime management
- **One-time parsing:** PredicateTree built once in init, not per row group
- **Cached decisions:** Row group bypass decisions computed once, reused in get_next()

## Debugging and Profiling

### Profile Counters
- **EQ-delete markers:** `EQDeleteMarker/{filter_id}/latency` (not JoinRuntimeFilter)
- **Statistics:** `_iceberg_eq_delete_row_groups_skipped`, `_iceberg_eq_delete_rows_skipped`

### Debug Output
- **RuntimeFilterProbeDescriptor:** `RFDptr(filter_id=123, rf=EQDeleteMarker(...))`
- **VLOG messages:** All prefixed with "EQDELETE" for easy filtering


## Memory Layout

### RuntimeFilterProbeDescriptor Contents
```cpp
class RuntimeFilterProbeDescriptor {
    int32_t _filter_id;                    // Filter ID for mapping
    ExprContext* _probe_expr_ctx;          // Column metadata (SlotId, LogicalType, name)
    std::atomic<const RuntimeFilter*> _runtime_filter;  // InRuntimeFilter with hash set
    TPlanNodeId _probe_plan_node_id;       // Target scan node ID
    // + standard RF fields (join_mode, layout, etc.)
};
```

### FileReader EQ-delete State
```cpp
class FileReader {
    std::vector<bool> _row_group_bypass_decisions;    // Cached per row group
    PredicateList _eq_delete_predicates;              // ColumnPredicate objects
    std::unique_ptr<PredicateTree> _eq_delete_predicate_tree;  // Evaluation tree
    ObjectPool _eq_delete_marker_pool;                // Memory management
};
```

## Integration Points

### Standard RuntimeFilter Integration
- **Coexistence:** EQ-delete markers delivered via RuntimeState, standard RF via RuntimeFilterProbeCollector
- **Skipped in ProbeCollector:** Standard evaluate() methods skip EQ_DELETE_MARKER filters
- **Profile separation:** Different counter names and debug strings

### Existing Infrastructure Reuse
- **RuntimeColumnPredicateBuilder:** Converts InRuntimeFilter → ColumnPredicate
- **PredicateFilterEvaluator:** Evaluates predicates against row group statistics
- **PredicateTree:** Standard tree structure for complex predicate evaluation
- **ConnectorPredicateParser:** Standard column metadata resolution

## Error Handling

### Graceful Degradation
- **Parser errors:** Fall back to no bypass (safe default)
- **Empty filters:** No bypass optimization (correct semantic)
- **Missing metadata:** Skip optimization for that column
- **Evaluation errors:** Return false (no bypass, safe default)

## Thread Safety

### Atomic Operations
- **RuntimeFilterProbeDescriptor:** Uses `std::atomic<const RuntimeFilter*>` for filter storage
- **FileReader state:** No shared mutable state during get_next() execution

### Memory Safety
- **Fragment ObjectPool:** Ensures lifetime exceeds HashJoiner execution
- **Proper initialization:** All parsing done in init phase, not runtime

---

**Architecture Document Version:** 1.0
**Date:** September 2025
**Status:** Implementation Complete
