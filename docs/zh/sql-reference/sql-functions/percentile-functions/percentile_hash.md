---
displayed_sidebar: docs
description: "将 DOUBLE 类型数值构造为 PERCENTILE 类型数值。"
---

# percentile_hash



将 `double` 类型数值构造成 `percentile` 类型数值。

## 语法

```plaintext
PERCENTILE_HASH(x[, compression]);
```

## 参数说明

`x`: 支持的数据类型为 DOUBLE。

`compression`: 可选常量表达式，结果必须是整数，范围为 [100, 10000]。支持 `5000.0` 等小数写法、类型转换和常量算术，只要结果为整数即可。非整数值及非常量表达式会报错。省略此参数或指定 `NULL` 时使用 `1000`。小于 `100` 的整数使用 `100`，大于 `10000` 的整数使用 `10000`。

使用双参数形式前，请将所有 BE 和 CN 节点升级到支持该形式的版本。

## 返回值说明

返回值的数据类型为 PERCENTILE。

## 示例

```Plain Text
mysql> select percentile_approx_raw(percentile_hash(234.234), 0.99);
+-------------------------------------------------------+
| percentile_approx_raw(percentile_hash(234.234), 0.99) |
+-------------------------------------------------------+
|                                    234.23399353027344 |
+-------------------------------------------------------+
1 row in set (0.00 sec)
```
