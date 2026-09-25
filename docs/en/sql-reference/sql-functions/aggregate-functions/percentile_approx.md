---
displayed_sidebar: docs
description: "Returns the approximate value for a given percentile p, or an array of values for corresponding percentiles if p is an array."
---

# percentile_approx



Returns the approximate value for a given percentile p, or an array of values for corresponding percentiles if p is an array. All percentile values must be in the range [0,1].

Compression is optional and ranges from 100 to 10000, with a default of 1000. Larger values use more memory and calculation time to retain more detail. Out-of-range integer values are clamped to the nearest bound.

This function uses fixed size memory, so less memory can be used for columns with high cardinality, and can be used to calculate statistics such as tp99.

## Syntax

```plaintext
DOUBLE PERCENTILE_APPROX(expr, DOUBLE|ARRAY<DOUBLE> p[, DOUBLE compression])
```

`compression` must be a constant expression with an integer value. `5000`, `5000.0`, `CAST(5000 AS DOUBLE)`, and `2500 * 2` are accepted. Fractional values such as `5000.5` and non-constant expressions are rejected without rounding. An omitted argument or `NULL` uses the default `1000`. Integers below `100` use `100`; integers above `10000` use `10000`.

## Examples

```plain text
MySQL > select `table`, percentile_approx(cost_time,0.99)
from log_statis
group by `table`;
+----------+--------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99) |
+----------+--------------------------------------+
| test     |                                54.22 |
+----------+--------------------------------------+

MySQL > select `table`, percentile_approx(cost_time,0.99, 4096)
from log_statis
group by `table`;
+----------+----------------------------------------------+
| table    | percentile_approx(`cost_time`, 0.99, 4096.0) |
+----------+----------------------------------------------+
| test     |                                        54.21 |
+----------+----------------------------------------------+

MySQL > select percentile_approx(c2, [0.1, 0.5, 0.9], 10000) from t1;
+-----------------------------------------------+
| percentile_approx(c2, [0.1, 0.5, 0.9], 10000) |
+-----------------------------------------------+
| [4999.6005859375,25000,45000.3984375]         |
+-----------------------------------------------+
```

## keyword

PERCENTILE_APPROX,PERCENTILE,APPROX
