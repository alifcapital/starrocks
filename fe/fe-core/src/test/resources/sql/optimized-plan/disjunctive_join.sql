[sql]
select * from t0 join t1 on t0.v1 = t1.v4 or t0.v2 = t1.v5;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 left join t1 on t0.v1 = t1.v4 or t0.v2 = t1.v5;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 right join t1 on t0.v1 = t1.v4 or t0.v2 = t1.v5;
[result]
RIGHT OUTER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 full outer join t1 on t0.v1 = t1.v4 or t0.v2 = t1.v5;
[result]
FULL OUTER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 join t1 on t0.v1 = t1.v4 or t0.v1 = t1.v5;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4 OR 1: v1 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 join t1 on t0.v1 = t1.v4 or t0.v2 = t1.v4;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 join t1 on t0.v1 = t1.v4 or t0.v2 = t1.v5 or t0.v3 = t1.v6;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 = 5: v5 OR 3: v3 = 6: v6] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 join t1 on (t0.v1 = t1.v4 and t0.v3 > 10) or (t0.v2 = t1.v5 and t0.v3 < 5);
[result]
INNER JOIN (join-predicate [(1: v1 = 4: v4 AND 3: v3 > 10) OR (2: v2 = 5: v5 AND 3: v3 < 5)] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from t0 join t1 on t0.v1 = t1.v4 or t0.v2 <=> t1.v5;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 <=> 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]
