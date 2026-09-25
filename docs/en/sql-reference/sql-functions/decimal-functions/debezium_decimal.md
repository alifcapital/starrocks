---
displayed_sidebar: docs
---

# debezium_decimal

Decodes Debezium VariableScaleDecimal (`STRUCT<scale INT, value VARBINARY>`) directly into an exact SQL decimal. Available in the custom 4.1 build.

```SQL
debezium_decimal(input, precision, scale)
```

`precision` and `scale` are integer literals: `1 <= precision <= 38`, `0 <= scale <= precision`. The return type is `DECIMAL(precision, scale)` (DECIMAL128 storage).

The source `value` is a signed, big-endian two's-complement integer. Its numeric value is `value * 10^(-input.scale)`. Source scale may vary between rows, including negative values. No conversion through DOUBLE or JSON is performed.

A NULL input or NULL member returns NULL. Empty binary input is invalid (zero is encoded as `00`). Overflow and loss of nonzero fractional digits raise an error regardless of `sql_mode`; trailing zero fractional digits can be removed exactly. Wide source integers are accepted when exact rescaling fits the target type.

```SQL
SELECT debezium_decimal(named_struct('scale', CAST(2 AS INT), 'value', hex_decode_binary('3A4E')), 10, 2);
-- 149.26

SELECT debezium_decimal(amount, 38, 6) AS amount
FROM iceberg.landing_cgw_procardpay_tj.p2p_credit;
```

Expose these expressions in a view to let users query ordinary numeric columns. This function does not change the Iceberg schema or stored data, and does not enable numeric predicate pushdown into the underlying binary field.
