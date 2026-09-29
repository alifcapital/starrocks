-- name: test_large_in_predicate_types
CREATE TABLE lip (
    k INT,
    ti TINYINT,
    si SMALLINT,
    i INT,
    bi BIGINT,
    v VARCHAR(30),
    c CHAR(5),
    d32 DECIMAL(9,2),
    d64 DECIMAL(18,4),
    d128 DECIMAL(38,6),
    dt DATE,
    dtt DATETIME,
    f DOUBLE
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num'='1');
INSERT INTO lip VALUES
    (1, 1, 1, 1, 1, '1', '1', 1.50, 1.5, 1.5, '2024-01-01', '2024-01-01 00:00:00', 1.5),
    (2, -1, 300, 40000, 3000000000, '01', 'ab', -2.25, 0.0001, 12345678901234567890.123456, '2024-02-29', '2024-02-29 12:34:56.000007', -2.25),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (4, 127, -300, -7, 20240101, 'abc', 'x', 0.00, 99999999999999.9999, -1, '0001-01-01', '9999-12-31 23:59:59', 0),
    (5, 2, 2, 2, 2, ' 7', '7', 7, 7, 7, '2024-01-02', '2024-01-02 00:00:00', 7),
    (6, 0, 0, 20240102, -3000000000, '20218840600116072801', 'abc', 1.23, -1.2345, 0.000001, '2024-01-03', '2024-01-02 03:04:05', 1),
    (7, -128, 32767, 2147483647, 9223372036854775807, '-5', 'x', 9999999.99, 1.5, 1.5, '2024-03-01', '2024-03-01 00:00:01', -0.0);
set large_in_predicate_threshold = 3;
set cbo_eq_base_type = 'decimal';
select k from lip where ti in (1, 2, -1, 127) order by k;
select k from lip where ti not in (1, 2, -1, 127) order by k;
select k from lip where si in (1, 300, -300, 70000) order by k;
select k from lip where si not in (1, 300, -300, 70000) order by k;
select k from lip where i in (1, 40000, -7, 3000000000) order by k;
select k from lip where bi in (1, 3000000000, 9223372036854775807, -3000000000) order by k;
select k from lip where bi in (20218840600116072801, 1, 2, 3) order by k;
select k from lip where i in ('1', '2', '40000', '-7') order by k;
select k from lip where i in ('01', '2', '40000', '-7') order by k;
select k from lip where i not in ('01', '2', '40000', '-7') order by k;
select k from lip where i in ('01', '2', '40000', 'abc') order by k;
select k from lip where i in ('01', '2', '100000000000000000000000000000000000000') order by k;
select k from lip where i not in ('01', '2', '100000000000000000000000000000000000000') order by k;
select k from lip where i in (1.0, 2.5, 40000, -7) order by k;
select k from lip where i not in (1.0, 2.5, 40000, -7) order by k;
select k from lip where v in (1, 7, -5, 20218840600116072801) order by k;
select k from lip where v in ('1', '01', ' 7', 'abc') order by k;
select k from lip where v not in ('1', '01', ' 7', 'abc') order by k;
select k from lip where v in (1.5, 7, 2, 3) order by k;
select k from lip where c in ('1', 'ab', 'x', 'abc') order by k;
select k from lip where c in (1, 7, 8, 9) order by k;
select k from lip where c not in (1, 7, 8, 9) order by k;
select k from lip where d32 in (1.5, -2.25, 0, 7) order by k;
select k from lip where d32 in (1.50, -2.250, 0.000, 1.23) order by k;
select k from lip where d32 not in (1.5, -2.25, 0, 7) order by k;
select k from lip where d32 in (1.234, 1.5, 7, 9999999.99) order by k;
select k from lip where d64 in (1.5, 0.0001, 7, -1.2345) order by k;
select k from lip where d64 in (99999999999999.9999, 1, 2, 3) order by k;
select k from lip where d128 in (12345678901234567890.123456, 1.5, -1, 0.000001) order by k;
select k from lip where d32 in ('1.5', '-2.25', '7', '9999999.99') order by k;
select k from lip where d32 in ('1.5', '-2.25', '7', 'x') order by k;
select k from lip where d32 in (99999999999999999999999999999999999999, 1.5, 7, 0) order by k;
select k from lip where d32 not in (99999999999999999999999999999999999999, 1.5, 7, 0) order by k;
select k from lip where dt in ('2024-01-01', '2024-02-29', '20240102', '0001-01-01') order by k;
select k from lip where dt not in ('2024-01-01', '2024-02-29', '20240102', '0001-01-01') order by k;
select k from lip where dt in ('2024-02-30', '2024-01-01', '2024-01-03', 'x') order by k;
select k from lip where dt in (20240101, 20240102, 20240103, 1) order by k;
select k from lip where dtt in ('2024-01-01', '2024-02-29 12:34:56.000007', '9999-12-31 23:59:59', '2024-01-02 03:04:05') order by k;
select k from lip where dtt in ('2024-02-29 12:34:56', '2024-01-02', '2024-03-01 00:00:01', '2024-01-01 00:00:00') order by k;
select k from lip where dtt not in ('2024-02-29 12:34:56', '2024-01-02', '2024-03-01 00:00:01', '2024-01-01 00:00:00') order by k;
select k from lip where i + 1 in (2, 40001, -6, 3) order by k;
select k from lip where i in (1, 2, 3, 4) and v in ('1', ' 7', 'abc', 'x') order by k;
select k from lip where i in (1, 2, -7, 4) and (v = 'abc' or k = 1) order by k;
select k from lip where not (i in (1, 2, -7, 4)) order by k;
select k from lip where not (i not in (1, 2, -7, 4)) order by k;
select k from lip where i in (1, 2, -7, 4) or k = 3 order by k;
select k, i in (1, 2, -7, 4) from lip order by k;
select k from lip where f in (1.5, -2.25, 0, 7) order by k;
select k from lip where f in (-0.0, 1, 3, 7) order by k;

-- name: test_large_in_predicate_types_plan
CREATE TABLE lip (
    k INT,
    i INT,
    v VARCHAR(30),
    d32 DECIMAL(9,2),
    dt DATE,
    dtt DATETIME,
    f DOUBLE
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num'='1');
INSERT INTO lip VALUES (1, 1, '1', 1.5, '2024-01-01', '2024-01-01 00:00:00', 1.5);
set enable_large_in_predicate = true;
set large_in_predicate_threshold = 3;
set cbo_eq_base_type = 'decimal';
function: assert_explain_contains("select k from lip where i in ('1', '2', '40000', '-7')", 'RAW VALUES', 'constant type: INT')
function: assert_explain_contains("select k from lip where i in ('01', '2', '40000', '-7')", 'RAW VALUES', 'constant type: DECIMAL')
function: assert_explain_contains("select k from lip where v in (1, 7, -5, 20218840600116072801)", 'RAW VALUES', 'constant type: VARCHAR')
function: assert_explain_contains("select k from lip where d32 in (1.5, -2.25, 0, 7)", 'RAW VALUES', 'constant type: DECIMAL')
function: assert_explain_contains("select k from lip where dt in ('2024-01-01', '2024-02-29', '20240102', '0001-01-01')", 'RAW VALUES', 'constant type: DATE')
function: assert_explain_contains("select k from lip where dtt in ('2024-01-01', '2024-02-29 12:34:56.000007', '9999-12-31 23:59:59')", 'RAW VALUES', 'constant type: DATETIME')
function: assert_explain_contains("select k from lip where i in (1, 2, -7, 4) and (v = 'abc' or k = 1)", 'RAW VALUES', 'LEFT SEMI JOIN')
function: assert_explain_contains("select k from lip where not (i in (1, 2, -7, 4))", 'RAW VALUES', 'LEFT ANTI JOIN')
function: assert_explain_contains("select k from lip where i in ('01', '2', '100000000000000000000000000000000000000')", 'RAW VALUES', 'constant count: 2')
function: assert_explain_contains("select k from lip where i not in ('01', '2', '100000000000000000000000000000000000000')", 'EMPTYSET')
function: assert_explain_not_contains("select k from lip where d32 in (99999999999999999999999999999999999999, 1.5, 7, 0)", 'RAW VALUES')
function: assert_explain_not_contains("select k from lip where i in ('01', '2', '40000', 'abc')", 'RAW VALUES')
function: assert_explain_not_contains("select k from lip where f in (1.5, -2.25, 0, 7)", 'RAW VALUES')
function: assert_explain_not_contains("select k from lip where i in (1, 2, -7, 4) or k = 3", 'RAW VALUES')
function: assert_explain_not_contains("select k, i in (1, 2, -7, 4) from lip", 'RAW VALUES')
set cbo_eq_base_type = 'varchar';
function: assert_explain_contains("select k from lip where i in ('01', '2', '40000', 'abc')", 'RAW VALUES', 'constant type: VARCHAR')
