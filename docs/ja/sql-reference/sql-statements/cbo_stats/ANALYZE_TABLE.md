---
displayed_sidebar: docs
description: "CBO 統計情報を収集するための手動収集タスクを作成します。"
---

# ANALYZE TABLE

## 説明

CBO 統計情報を収集するための手動収集タスクを作成します。デフォルトでは、手動収集は同期操作です。非同期操作に設定することもできます。非同期モードでは、ANALYZE TABLE を実行した後、システムはこのステートメントが成功したかどうかをすぐに返します。ただし、収集タスクはバックグラウンドで実行され、結果を待つ必要はありません。タスクのステータスは SHOW ANALYZE STATUS を実行して確認できます。非同期収集はデータ量の多いテーブルに適しており、同期収集はデータ量の少ないテーブルに適しています。

**手動収集タスクは作成後に一度だけ実行されます。手動収集タスクを削除する必要はありません。**

このステートメントは v2.4 からサポートされています。

### 基本統計情報を手動で収集する

基本統計情報の詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#basic-statistics) を参照してください。

#### 構文

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
PROPERTIES (property [,property])
```

#### パラメーターの説明

- 収集タイプ
  - FULL: 完全収集を示します。
  - SAMPLE: サンプル収集を示します。
  - 収集タイプが指定されていない場合、デフォルトで完全収集が使用されます。

- `col_name`: 統計情報を収集する列。複数の列はカンマ（`,`）で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。パラメーターを指定しない場合、デフォルトで同期収集が使用されます。

- `PROPERTIES`: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` ファイルのデフォルト設定が使用されます。実際に使用されるプロパティは、SHOW ANALYZE STATUS の出力の `Properties` 列で確認できます。

| **PROPERTIES**                | **Type** | **Default value** | **Description**                                              |
| ----------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000            | サンプル収集のために収集する最小行数。このパラメーターの値がテーブル内の実際の行数を超える場合、完全収集が実行されます。 |

#### 例

例 1: 手動での完全収集

```SQL
-- デフォルト設定を使用してテーブルの完全な統計情報を手動で収集します。
ANALYZE TABLE tbl_name;

-- デフォルト設定を使用してテーブルの完全な統計情報を手動で収集します。
ANALYZE FULL TABLE tbl_name;

-- デフォルト設定を使用してテーブル内の指定された列の統計情報を手動で収集します。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

例 2: 手動でのサンプル収集

```SQL
-- デフォルト設定を使用してテーブルの部分的な統計情報を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name;

-- 収集する行数を指定して、テーブル内の指定された列の統計情報を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

### ヒストグラムを手動で収集する

ヒストグラムの詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#histogram) を参照してください。

#### 構文

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
PROPERTIES (property [,property]);
```

#### パラメーターの説明

- `col_name`: 統計情報を収集する列。複数の列はカンマ（`,`）で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。ヒストグラムにはこのパラメーターが必要です。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。パラメーターを指定しない場合、デフォルトで同期収集が使用されます。

- `WITH N BUCKETS`: ヒストグラム収集のためのバケット数 `N`。指定されていない場合、`fe.conf` のデフォルト値が使用されます。

- PROPERTIES: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。実際に使用されるプロパティは、SHOW ANALYZE STATUS の出力の `Properties` 列で確認できます。

| **PROPERTIES**                 | **Type** | **Default value** | **Description**                                              |
| ------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows  | INT      | 200000            | 収集する最小行数。このパラメーターの値がテーブル内の実際の行数を超える場合、完全収集が実行されます。 |
| histogram_buckets_size         | LONG     | 64                | ヒストグラムのデフォルトのバケット数。                       |
| histogram_mcv_size             | INT      | 100               | ヒストグラムの最も一般的な値 (MCV) の数。                    |
| histogram_sample_ratio         | FLOAT    | 0.1               | ヒストグラムのサンプリング比率。                             |
| histogram_max_sample_row_count | LONG     | 10000000          | ヒストグラムのために収集する最大行数。                       |

ヒストグラムのために収集する行数は、複数のパラメーターによって制御されます。それは `statistic_sample_collect_rows` とテーブル行数 * `histogram_sample_ratio` の間の大きい方の値です。この数は `histogram_max_sample_row_count` で指定された値を超えることはできません。値が超えた場合、`histogram_max_sample_row_count` が優先されます。

#### 例

```SQL
-- デフォルト設定を使用して v1 のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 32 バケット、32 MCV、および 50% のサンプリング比率で v1 および v2 のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

### 外部テーブルの MCV 統計を収集する

```sql
ANALYZE [FULL] TABLE catalog.db.table MCV (column [, column ...])
[PROPERTIES ("mcv_size" = "100", "mcv_bucket_num" = "64")];

SHOW MCV STATS META;
DROP MCV STATS catalog.db.table;
DROP MCV STATS catalog.db.table (column [, column ...]);
```

MCV 統計は、単一列または列グループの頻度分布を表します。指定した列を2回スキャンし、最初に Sketch で頻出候補と数値・日時の単一列のバケット境界を求め、次に候補、NULL、バケットの行数を数えます。カウントは2回目のスキャンに対して正確であり、異なる値の数と境界は Sketch による推定です。メモリ使用量は入力の異なる値の数ではなく、Sketch、候補、バケットの設定に依存します。

- `mcv_size`: 保持する頻出タプルの最大数。デフォルトは FE 設定 `statistic_mcv_size` の値（100）です。
- `mcv_bucket_num`: 単一列の残余分布に対する目標バケット数。デフォルトは `statistic_mcv_bucket_num` の値（64）です。重複する境界によりバケット数が減る場合があります。数値・日時列は順序付きバケットを使用し、文字列・Boolean 列は残余行数と異なる値の数を使用します。
- 両属性は正の整数です。複数列のグループには `mcv_bucket_num` を指定できません。ヒストグラム用の属性は MCV には適用されません。

分析可能な外部テーブルに対する同期の全件収集のみをサポートします。トップレベルのスカラー列を指定してください。MCV は独立したストレージとライフサイクルを持ち、従来のヒストグラム収集は不要です。列リストを省略した `DROP MCV STATS` は対象テーブルの全 MCV グループを削除します。列リストを指定すると、列の順序に関係なく、その列集合に完全一致するグループだけを削除します。他の MCV グループと基本統計は保持されます。単一列の頻度、残余バケット、NULL 割合と、複数列の結合頻度をオプティマイザーが利用します。2回のスキャンで同じ外部スナップショットを固定する仕組みはありません。

## 参考文献

[SHOW ANALYZE STATUS](SHOW_ANALYZE_STATUS.md): 手動収集タスクのステータスを表示します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中の手動収集タスクをキャンセルします。

CBO の統計情報収集の詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。
