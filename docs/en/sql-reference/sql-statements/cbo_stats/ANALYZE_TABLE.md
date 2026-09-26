---
displayed_sidebar: docs
description: "ANALYZE TABLE creates a manual collection task for collecting CBO statistics."
---

# ANALYZE TABLE

ANALYZE TABLE creates a manual collection task for collecting CBO statistics. By default, manual collection is a synchronous operation. You can also set it to an asynchronous operation. In asynchronous mode, after you run ANALYZE TABLE, the system immediately returns whether this statement is successful. However, the collection task will be running in the background and you do not have to wait for the result. You can check the status of the task by running SHOW ANALYZE STATUS. Asynchronous collection is suitable for tables with large data volume, whereas synchronous collection is suitable for tables with small data volume.

**Manual collection tasks are run only once after creation. You do not need to delete manual collection tasks.**

This statement is supported from v2.4.

### Manually collect basic statistics

For more information about basic statistics, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#basic-statistics).

#### Syntax

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
PROPERTIES (property [,property])
```

#### Parameter description

- Collection type
  - FULL: indicates full collection.
  - SAMPLE: indicates sampled collection.
  - If no collection type is specified, full collection is used by default.

- `col_name`: columns from which to collect statistics. Separate multiple columns with commas (`,`). If this parameter is not specified, the entire table is collected.

- [WITH SYNC | ASYNC MODE]: whether to run the manual collection task in synchronous or asynchronous mode. Synchronous collection is used by default if you do not specify this parameter.

- `PROPERTIES`: custom parameters. If `PROPERTIES` is not specified, the default settings in the `fe.conf` file are used. The properties that are actually used can be viewed via the `Properties` column in the output of SHOW ANALYZE STATUS.

| **PROPERTIES**                | **Type** | **Default value** | **Description**                                              |
| ----------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000            | The minimum number of rows to collect for sampled collection.If the parameter value exceeds the actual number of rows in your table, full collection is performed. |

#### Examples

Example 1: Manual full collection

```SQL
-- Manually collect full stats of a table using default settings.
ANALYZE TABLE tbl_name;

-- Manually collect full stats of a table using default settings.
ANALYZE FULL TABLE tbl_name;

-- Manually collect stats of specified columns in a table using default settings.
ANALYZE TABLE tbl_name(c1, c2, c3);
```

Example 2: Manual sampled collection

```SQL
-- Manually collect partial stats of a table using default settings.
ANALYZE SAMPLE TABLE tbl_name;

-- Manually collect stats of specified columns in a table, with the number of rows to collect specified.
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

### Manually collect histograms

For more information about histograms, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#histogram).

#### Syntax

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
PROPERTIES (property [,property]);
```

#### Parameter description

- `col_name`: columns from which to collect statistics. Separate multiple columns with commas (`,`). If this parameter is not specified, the entire table is collected. This parameter is required for histograms.

- [WITH SYNC | ASYNC MODE]: whether to run the manual collection task in synchronous or asynchronous mode. Synchronous collection is used by default if you do not specify this parameter.

- `WITH N BUCKETS`: `N` is the number of buckets for histogram collection. If not specified, the default value in `fe.conf` is used.

- PROPERTIES: custom parameters. If `PROPERTIES` is not specified, the default settings in `fe.conf` are used. The properties that are actually used can be viewed via the `Properties` column in the output of SHOW ANALYZE STATUS.

| **PROPERTIES**                 | **Type** | **Default value** | **Description**                                              |
| ------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows  | INT      | 200000            | The minimum number of rows to collect. If the parameter value exceeds the actual number of rows in your table, full collection is performed. |
| histogram_buckets_size         | LONG     | 64                | The default bucket number for a histogram.                   |
| histogram_mcv_size             | INT      | 100               | The number of most common values (MCV) for a histogram.      |
| histogram_sample_ratio         | FLOAT    | 0.1               | The sampling ratio for a histogram.                          |
| histogram_max_sample_row_count | LONG     | 10000000          | The maximum number of rows to collect for a histogram.       |

The number of rows to collect for a histogram is controlled by multiple parameters. It is the larger value between `statistic_sample_collect_rows` and table row count * `histogram_sample_ratio`. The number cannot exceed the value specified by `histogram_max_sample_row_count`. If the value is exceeded, `histogram_max_sample_row_count` takes precedence.

#### Examples

```SQL
-- Manually collect histograms on v1 using the default settings.
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- Manually collect histograms on v1 and v2, with 32 buckets, 32 MCVs, and 50% sampling ratio.
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

### Collect MCV statistics for external tables

```sql
ANALYZE [FULL] TABLE catalog.db.table MCV
    { (column [, column ...]) | PREDICATE COLUMNS }
[PROPERTIES ("mcv_size" = "100", "mcv_bucket_num" = "64")];

SHOW MCV STATS META;
DROP MCV STATS catalog.db.table;
DROP MCV STATS catalog.db.table (column [, column ...]);
```

MCV statistics describe the frequency distribution of one column or a column group. Collection scans the selected columns twice: sketches identify frequent candidates and, for a numeric, date/time, or string singleton, residual bucket boundaries; a second pass counts the candidates, NULLs, and buckets. Counts are exact for the counting pass; distinct counts and candidate boundaries are estimated by sketches. Memory depends on the configured sketch, candidate, and bucket sizes rather than the number of distinct input tuples.

- `mcv_size`: maximum number of frequent tuples to retain. Default: FE configuration `statistic_mcv_size` (100).
- `mcv_bucket_num`: target number of residual buckets for a single column. Default: `statistic_mcv_bucket_num` (64). Boundaries can collapse, so fewer buckets may be produced. Numeric, date/time, and string columns have ordered buckets. String buckets use string ordering; Boolean columns use residual mass and distinct count without ordered buckets.
- `mcv_size` accepts positive integers; `mcv_bucket_num` accepts integers from 1 to 10,000. `mcv_bucket_num` requires at least one single-column group and applies only to those groups. Histogram properties do not apply to MCV collection.

Only synchronous, full collection on supported external tables is available. Specify one or more top-level scalar columns. MCV statistics have their own storage and lifecycle; collecting a legacy histogram is not required. `DROP MCV STATS` without a column list removes all collected MCV groups of the table. With a column list, it removes only that exact group, regardless of column order; other MCV groups and basic statistics are preserved.

A single-column record supplies frequent values, residual buckets, distinct count, and NULL frequency to the optimizer. Multi-column records also supply joint frequencies and component counts for correlated predicates. The optimizer evaluates known frequent tuples and estimates the remaining population separately. For Iceberg, all groups and both passes use one captured snapshot. Other connectors use their normal read semantics.

#### Select groups from predicate usage

```sql
ANALYZE FULL TABLE iceberg.landing_mobi_tj.transactions MCV PREDICATE COLUMNS;
```

This command collects each distinct eligible column set in the recent predicate-usage history. Recording
requires `enable_predicate_columns_collection` and `enable_external_predicate_columns_collection`.
The history includes filters, JOIN keys, GROUP BY/window partition keys, and DISTINCT arguments. It is
bounded usage history, not an exhaustive query log. It does not infer groups from the union of all columns.

For example, observations of `(status, provider_id)`, `(provider_id, extra_info)`,
`(status, provider_id, extra_info)`, and `(status)` produce four separate distributions. Repeated observations,
different column order, and different usage reasons do not duplicate collection. A superset does not replace
a subset: the retained frequent triples and residual population need not preserve the frequent pairs.
Singleton statistics are collected only when a singleton was observed independently.

A group is skipped as a whole if it references a deleted or unsupported column, or exceeds
`statistics_max_multi_column_combined_num`. If no eligible group remains, the command returns an error;
it does not fall back to collecting all columns. The command does not delete previously collected groups.
Use `SHOW MCV STATS META` to inspect collected sets.

Groups share two scans in sequential batches of up to `statistic_mcv_max_groups_per_scan` (default: 4).
Each scan uses one aggregate operator with independent state for each group. This saves repeated input
reading; per-group sketching/counting CPU and memory remain. Memory also depends on value lengths and
execution parallelism. Set the limit to 1 for separate scans. Completed batches are written before the next
batch starts, so cancellation or failure can leave some distributions refreshed. Re-running the command
refreshes every selected group.

## References

[SHOW ANALYZE STATUS](SHOW_ANALYZE_STATUS.md): view the status of a manual collection task.

[KILL ANALYZE](KILL_ANALYZE.md): cancel a manual collection task that is running.

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
