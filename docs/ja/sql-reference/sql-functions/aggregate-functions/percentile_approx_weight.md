---
displayed_sidebar: docs
description: "指定されたパーセンタイルの加重近似値を返します。PERCENTILE_APPROX の重み付きバージョンで、各入力値に重みを指定できます。"
---

# percentile_approx_weight

指定されたパーセンタイルの加重近似値を返します。パーセンタイルパラメータ p は単一の値または配列を指定できます。`percentile_approx_weight` は `PERCENTILE_APPROX` の重み付きバージョンであり、各入力値に対して重み（定数値または数値列）を指定することができます。

この関数は固定サイズのメモリを使用するため、高いカーディナリティを持つ列に対してメモリ使用量を抑えることができます。また、tp99 などの統計を計算するためにも使用できます。

## 構文

```plaintext
DOUBLE PERCENTILE_APPROX_WEIGHT(expr, BIGINT weight, DOUBLE|ARRAY<DOUBLE> p[, DOUBLE compression])
```

- `expr`: パーセンタイルを計算する列。
- `p` : サポートされるデータ型はDOUBLEであり、pの値は0から1の間で指定します。配列タイプ`ARRAY<DOUBLE>`の場合、配列の各値は0から1の間でなければなりません。例えば、0.99は99番目のパーセンタイルを表します。
- `weight` : 重み列。正の定数値または列でなければなりません。
- 圧縮パラメータは省略可能で、範囲は [100, 10000]、デフォルト値は 1000 です。値が大きいほど多くの情報を保持しますが、メモリ消費量と計算時間が増加します。範囲外の整数には最も近い境界値を使用します。

`compression` には、評価結果が整数となる定数式を指定します。`5000`、`5000.0`、`CAST(5000 AS DOUBLE)`、`2500 * 2` を使用できます。`5000.5` などの小数部分を持つ値や定数でない式は、丸めずにエラーとします。引数を省略した場合、または `NULL` を指定した場合は、デフォルト値 `1000` を使用します。`100` 未満の整数には `100`、`10000` を超える整数には `10000` を使用します。

## 例

```plain text
CREATE TABLE t1 (
    c1 int,
    c2 double,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string,
    c8 double,
    c9 date,
    c10 datetime,
    c11 array<int>,
    c12 map<double, double>,
    c13 struct<a bigint, b double>
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
insert into t1 
    select generate_series, generate_series,  11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)
    from table(generate_series(1, 50000, 3));
-- 定数値を重みとして使用
mysql> select percentile_approx_weighted(c1, 1, 0.9) from t1;
+----------------------------------------+
| percentile_approx_weighted(c1, 1, 0.9) |
+----------------------------------------+
|                         45000.39453125 |
+----------------------------------------+
1 row in set (0.07 sec)
-- 数値列を重みとして使用
mysql> select percentile_approx_weighted(c2, c1, 0.5) from t1;
+-----------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5) |
+-----------------------------------------+
|                          35355.97265625 |
+-----------------------------------------+
1 row in set (0.07 sec)
-- 重みと圧縮を使用してパーセンタイルを計算
mysql> select percentile_approx_weighted(c2, c1, 0.5, 10000) from t1;
+------------------------------------------------+
| percentile_approx_weighted(c2, c1, 0.5, 10000) |
+------------------------------------------------+
|                                 35355.97265625 |
+------------------------------------------------+
1 row in set (0.09 sec)

mysql> select percentile_approx_weighted(c2, c1, [0.1, 0.5, 0.9], 10000) from t1;
+------------------------------------------------------------+
| percentile_approx_weighted(c2, c1, [0.1, 0.5, 0.9], 10000) |
+------------------------------------------------------------+
| [15811.6708984375,35355.97265625,47435.01171875]           |
+------------------------------------------------------------+
1 row in set (0.03 sec)
```

## キーワード

PERCENTILE_APPROX_WEIGHT,PERCENTILE_APPROX,PERCENTILE,APPROX
