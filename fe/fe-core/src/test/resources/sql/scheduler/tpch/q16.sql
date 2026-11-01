[scheduler]
PLAN FRAGMENT 0(F06)
  DOP: 16
  INSTANCES
    INSTANCE(0-F06#0)
      BE: 10003

PLAN FRAGMENT 1(F05)
  DOP: 16
  INSTANCES
    INSTANCE(1-F05#0)
      DESTINATIONS: 0-F06#0
      BE: 10001
    INSTANCE(2-F05#1)
      DESTINATIONS: 0-F06#0
      BE: 10002
    INSTANCE(3-F05#2)
      DESTINATIONS: 0-F06#0
      BE: 10003

PLAN FRAGMENT 2(F04)
  DOP: 16
  INSTANCES
    INSTANCE(4-F04#0)
      DESTINATIONS: 1-F05#0,2-F05#1,3-F05#2
      BE: 10003
    INSTANCE(5-F04#1)
      DESTINATIONS: 1-F05#0,2-F05#1,3-F05#2
      BE: 10002
    INSTANCE(6-F04#2)
      DESTINATIONS: 1-F05#0,2-F05#1,3-F05#2
      BE: 10001

PLAN FRAGMENT 3(F00)
  DOP: 16
  INSTANCES
    INSTANCE(7-F00#0)
      DESTINATIONS: 4-F04#0,5-F04#1,6-F04#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        0:OlapScanNode
          1. partitionID=2488,tabletID=2491
          2. partitionID=2488,tabletID=2497
          3. partitionID=2488,tabletID=2503
          4. partitionID=2488,tabletID=2509
          5. partitionID=2488,tabletID=2515
          6. partitionID=2488,tabletID=2521
        1:OlapScanNode
          1. partitionID=2528,tabletID=2531
          2. partitionID=2528,tabletID=2537
          3. partitionID=2528,tabletID=2543
          4. partitionID=2528,tabletID=2549
          5. partitionID=2528,tabletID=2555
          6. partitionID=2528,tabletID=2561
    INSTANCE(8-F00#1)
      DESTINATIONS: 4-F04#0,5-F04#1,6-F04#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        0:OlapScanNode
          1. partitionID=2488,tabletID=2493
          2. partitionID=2488,tabletID=2499
          3. partitionID=2488,tabletID=2505
          4. partitionID=2488,tabletID=2511
          5. partitionID=2488,tabletID=2517
          6. partitionID=2488,tabletID=2523
        1:OlapScanNode
          1. partitionID=2528,tabletID=2533
          2. partitionID=2528,tabletID=2539
          3. partitionID=2528,tabletID=2545
          4. partitionID=2528,tabletID=2551
          5. partitionID=2528,tabletID=2557
          6. partitionID=2528,tabletID=2563
    INSTANCE(9-F00#2)
      DESTINATIONS: 4-F04#0,5-F04#1,6-F04#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        0:OlapScanNode
          1. partitionID=2488,tabletID=2489
          2. partitionID=2488,tabletID=2495
          3. partitionID=2488,tabletID=2501
          4. partitionID=2488,tabletID=2507
          5. partitionID=2488,tabletID=2513
          6. partitionID=2488,tabletID=2519
        1:OlapScanNode
          1. partitionID=2528,tabletID=2529
          2. partitionID=2528,tabletID=2535
          3. partitionID=2528,tabletID=2541
          4. partitionID=2528,tabletID=2547
          5. partitionID=2528,tabletID=2553
          6. partitionID=2528,tabletID=2559

PLAN FRAGMENT 4(F02)
  DOP: 16
  INSTANCES
    INSTANCE(10-F02#0)
      DESTINATIONS: 7-F00#0,8-F00#1,9-F00#2
      BE: 10001
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=2574,tabletID=2579
          2. partitionID=2574,tabletID=2585
          3. partitionID=2574,tabletID=2591
          4. partitionID=2574,tabletID=2597
    INSTANCE(11-F02#1)
      DESTINATIONS: 7-F00#0,8-F00#1,9-F00#2
      BE: 10002
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=2574,tabletID=2575
          2. partitionID=2574,tabletID=2581
          3. partitionID=2574,tabletID=2587
          4. partitionID=2574,tabletID=2593
    INSTANCE(12-F02#2)
      DESTINATIONS: 7-F00#0,8-F00#1,9-F00#2
      BE: 10003
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=2574,tabletID=2577
          2. partitionID=2574,tabletID=2583
          3. partitionID=2574,tabletID=2589
          4. partitionID=2574,tabletID=2595

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:9: p_brand | 10: p_type | 11: p_size | 23: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  16:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 9: p_brand, 10: p_type, 11: p_size

  STREAM DATA SINK
    EXCHANGE ID: 16
    UNPARTITIONED

  15:SORT
  |  order by: <slot 23> 23: count DESC, <slot 9> 9: p_brand ASC, <slot 10> 10: p_type ASC, <slot 11> 11: p_size ASC
  |  offset: 0
  |  
  14:AGGREGATE (merge finalize)
  |  output: count(23: count)
  |  group by: 9: p_brand, 10: p_type, 11: p_size
  |  
  13:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size

  STREAM DATA SINK
    EXCHANGE ID: 13
    HASH_PARTITIONED: 9: p_brand, 10: p_type, 11: p_size

  12:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(2: ps_suppkey)
  |  group by: 9: p_brand, 10: p_type, 11: p_size
  |  
  11:AGGREGATE (merge serialize)
  |  group by: 2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size
  |  
  10:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 10
    HASH_PARTITIONED: 2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size

  9:AGGREGATE (update serialize)
  |  STREAMING
  |  group by: 2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size
  |  
  8:Project
  |  <slot 2> : 2: ps_suppkey
  |  <slot 9> : 9: p_brand
  |  <slot 10> : 10: p_type
  |  <slot 11> : 11: p_size
  |  
  7:HASH JOIN
  |  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 2: ps_suppkey = 15: s_suppkey
  |  
  |----6:EXCHANGE
  |    
  3:Project
  |  <slot 2> : 2: ps_suppkey
  |  <slot 9> : 9: p_brand
  |  <slot 10> : 10: p_type
  |  <slot 11> : 11: p_size
  |  
  2:HASH JOIN
  |  join op: INNER JOIN (COLOCATE)
  |  colocate: true
  |  equal join conjunct: 6: p_partkey = 1: ps_partkey
  |  
  |----1:OlapScanNode
  |       TABLE: partsupp
  |       PREAGGREGATION: ON
  |       partitions=1/1
  |       rollup: partsupp
  |       tabletRatio=18/18
  |       tabletList=2529,2531,2533,2535,2537,2539,2541,2543,2545,2547 ...
  |       cardinality=1
  |       avgRowSize=16.0
  |    
  0:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     PREDICATES: 9: p_brand != 'Brand#43', NOT (10: p_type LIKE 'PROMO BURNISHED%'), 11: p_size IN (31, 43, 9, 6, 18, 11, 25, 1)
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=2489,2491,2493,2495,2497,2499,2501,2503,2505,2507 ...
     cardinality=1
     avgRowSize=47.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 06
    UNPARTITIONED

  5:Project
  |  <slot 15> : 15: s_suppkey
  |  
  4:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     PREDICATES: 21: s_comment LIKE '%Customer%Complaints%'
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=2575,2577,2579,2581,2583,2585,2587,2589,2591,2593 ...
     cardinality=1
     avgRowSize=105.0
[end]