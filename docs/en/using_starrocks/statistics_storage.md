---
displayed_sidebar: docs
---

# Statistics storage and inspection

These tables are in `default_catalog._statistics_`. Read them with an administrative account. This patch adds prepared external BASIC summaries; it does not replace the readable native statistics or histogram formats.

| Table | Stored content | Inspect with ordinary SQL |
| --- | --- | --- |
| `column_statistics` | Native FULL statistics, with HLL in `ndv` | Scalar columns and `hll_cardinality(ndv)` |
| `table_statistic_v1` | Native SAMPLE scalar statistics, including extrapolated NDV | Scalar columns |
| `external_column_statistics` | External BASIC cells by table, partition and column, with HLL | Scalar columns and `hll_cardinality(ndv)` |
| `histogram_statistics`, `external_histogram_statistics` | Histogram buckets and most common values | Readable JSON text |
| `external_mcv_statistics` | One predicate column group per row | Scalar row count/NDV plus readable JSON tuples, frequencies, buckets and NULL counts |
| `external_table_statistics` | One prepared BASIC summary per table | `payload` is an array of versioned JSON strings; NDV is already scalar |
| `external_partition_statistics` | Optional prepared copy of all collected columns of one partition | Identity/time fields are readable; `payload` is Zstd-compressed binary containing scalars and HLL bytes |
| `join_statistics` | Parts of a collected JOIN-statistics generation | Object ID/generation/part/time fields are readable; `payload` is compressed binary |

## External BASIC cells

The cell store remains available for BE aggregation and inspection even when a packed partition copy exists:

```sql
SELECT partition_name, column_name, row_count,
       hll_cardinality(ndv) AS ndv, null_count, min, max, update_time
FROM default_catalog._statistics_.external_column_statistics
WHERE catalog_name = 'iceberg'
  AND db_name = 'landing_mobi_tj'
  AND table_name = 'transactions';
```

## Prepared TABLE summary

Unnest the JSON chunks to avoid printing an array of escaped strings:

```sql
SELECT s.update_time, u.chunk
FROM default_catalog._statistics_.external_table_statistics AS s,
     UNNEST(s.payload) AS u(chunk)
WHERE s.catalog_name = 'iceberg'
  AND s.db_name = 'landing_mobi_tj'
  AND s.table_name = 'transactions';
```

Each chunk has `version` and `columns`. Each column record contains these nine fields in order:

```text
[column_name, source_type, row_count, data_size, ndv,
 null_count, min, max, collected_at]
```

`update_time` is the summary publication time. `collected_at` is the individual column's collection time, which can differ after partial ANALYZE. The values are readable through JSON functions; no HLL decoder is needed for this TABLE summary.

## Local MCV

```sql
SELECT column_names, row_count, ndv, mcv, buckets, null_counts, update_time
FROM default_catalog._statistics_.external_mcv_statistics
WHERE catalog_name = 'iceberg'
  AND db_name = 'landing_mobi_tj'
  AND table_name = 'transactions';
```

`column_names` defines tuple component order. Each MCV entry is `[values, count, component_counts]`: the tuple values, its row count, and the marginal counts of each component when collected. JSON `null` represents SQL NULL. The stored JSON and prepared Java cache representation are different formats; compact Java objects do not make this SQL table binary.

## Packed partition and JOIN payloads

`hll_cardinality(payload)` is not applicable to either binary format: the payload is an entire compressed statistics object, not a single HLL. `to_base64(payload)` only transports the bytes and does not decode them.

For a packed partition, use `external_column_statistics` above to inspect the same underlying column statistics. JOIN statistics have no readable duplicate table, but FE provides a paginated SQL command:

```sql
-- Short collection status and generation metadata.
SHOW JOIN STATISTICS;
SHOW JOIN STATISTICS transactions_users;
-- Decoded distributions from the saved generation.
SHOW VERBOSE JOIN STATISTICS transactions_users LIMIT 100;
SHOW VERBOSE JOIN STATISTICS transactions_users LIMIT 100 OFFSET 100;
```

VERBOSE returns sections for source slices, degree moments, head keys/frequencies, tail norms and correlations. Details are readable JSON; SELECT is required on all source tables. The default page has 100 rows, the maximum is 1,000. See [JOIN statistics](join_statistics.md#inspect-collected-distributions) for field meanings and paging rules. No Java script or BE scalar decoder is needed for this command.

## Collection and deployment

ANALYZE publishes TABLE and available packed partition summaries from collected cells, after verifying write visibility. It does not rescan the source data for this publication stage. Oversized packed partition rows are omitted and the authoritative cell/block path remains available. External BASIC statistics must be recollected for the new normalized storage format; MCV, JOIN and histogram statistics are not deleted by this change. Native SAMPLE collection and its NDV estimator are independent of the external packed format.
