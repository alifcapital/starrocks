---
displayed_sidebar: docs
description: "DROP STATS deletes only basic CBO statistics."
---

# DROP STATS

DROP STATS deletes only basic CBO statistics. For more information, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#basic-statistics).

For both native and external tables, plain `DROP STATS` preserves multi-column statistics, MCV statistics, and histograms. Use the explicit commands below to remove those statistics.

You can delete statistical information you do not need. When you delete statistics, both the data and metadata of the statistics are deleted, as well as the statistics in expired cache. Note that if an automatic collection task is ongoing, previously deleted statistics may be collected again. You can use `SHOW ANALYZE STATUS` to view the history of collection tasks.

This statement is supported from v2.4.

## Syntax

### Delete basic statistics

```SQL
DROP STATS tbl_name
```

### Delete native multi-column statistics

```SQL
DROP MULTIPLE COLUMNS STATS tbl_name;
```

Basic statistics and histograms are preserved.

### Delete external MCV statistics

```SQL
DROP MCV STATS catalog.db.tbl_name;
DROP MCV STATS catalog.db.tbl_name (col_name [, col_name ...]);
```

Without a column list, all MCV groups of the table are removed. With a list, only that exact group is removed, regardless of column order. Basic statistics and histograms are preserved.

### Delete histograms

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name];
```

## References

For more information about collecting statistics for CBO, see [Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md).
