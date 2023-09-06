## TPC-C for JDBC

TPC-C for JDBC is a TPC-C benchmark (workload generator)  base on Java JDBC.

### Features
- Multiple relationship database support: MySQL, PostgreSQL, Oracle, DB2, MSSQL, Derby(Java DB), SQLite, H2. 
- Faster TPC-C data generate and easy to use.

### Install

Download the pre-built binary here([https://github.com/Li-Xiang/tpcc-jdbc/releases](https://github.com/Li-Xiang/tpcc-jdbc/releases)) and then unzip it. it requires minimum of Java 11 at runtime.

### Usage
```
tpcc.sh {benchmark|load|drop|addfk|dropfk|check} {benchmark-config-file}

command:
  benchmark: Run TPC-C benchmark.
  load     : Create and Load TPC-C tables and data.
  check    : Check TPC-C tables and data.
  drop     : Drop TPC-C tables and data.
  addfk    : Add foreign keys for TPC-C tables.
  dropfk   : Drop foreign keys for TPC-C tables.

benchmark-config-file: Specify benchmark configuration file.

examples:

D:\Github\tpcc-jdbc\dist>tpcc.cmd benchmark demos\benchmark-mysql.json
******************************************************************
TPC-C Load Generator (for JDBC)
  [DataSource]: com.zaxxer.hikari.HikariDataSource
  [Driver]    : MySQL Connector/J / mysql-connector-java-8.0.28 (Revision: 7ff2161da3899f379fb3171b6538b191b1c5c7e2)
  [URL]       : jdbc:mysql://127.0.0.1:3306/tpcc
  [DBMS]      : MySQL / 8.0.18
  [Warehouse] : 5
  [Threads]   : 5
******************************************************************

2023-09-06 17:32:16.244 INFO  o.l.t.TpccDriver: Ramp-up 6 sec...
         | Total  |    New-Order      |     Payment       |   Order-Status    |     Delivery      |    Stock-Level    |
         |  TPs/  | TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/| TPs/ AvgRt/ MaxRt/|
---------+--------+-------------------+-------------------+-------------------+-------------------+-------------------+
17:32:26 |    268 |   120    25    60 |   116     9    49 |    10     3    11 |    11    50    99 |    11     4    27 |
17:32:30 |    274 |   121    25    80 |   116     9    47 |    13     3    13 |    11    49    91 |    13     5    20 |
17:32:34 |    275 |   115    26    69 |   127     9    54 |    11     3    12 |    11    47    67 |    11     7    24 |
17:32:38 |    267 |   120    25    58 |   115     9    43 |     9     3    17 |    11    49   100 |    11     8    28 |
17:32:42 |    273 |   117    25    61 |   120     9    55 |    13     4    15 |    11    45    73 |    11     7    23 |
17:32:46 |    267 |   122    25    67 |   112     9    40 |    10     3    13 |    13    46    65 |    10    10    37 |
17:32:50 |    280 |   121    25    68 |   124     9    64 |    12     3    15 |    11    47    73 |    13     8    27 |
17:32:54 |    280 |   123    25    77 |   124     9    72 |    12     3     9 |    10    42    65 |    11     7    24 |
17:32:58 |    271 |   122    25    96 |   116     9    39 |    14     2     7 |    11    45    69 |     9     9    19 |
17:33:02 |    221 |   103    29    76 |    93    14    64 |    11     3    10 |     7    56   102 |     8    14    43 |
17:33:06 |    150 |    68    39    93 |    65    25    78 |     4     7    14 |     7    58   129 |     5    26    66 |
17:33:10 |    151 |    69    39   119 |    62    24   102 |     6     7    20 |     8    61   107 |     7    25    70 |
17:33:14 |    147 |    70    39   132 |    62    26   112 |     5     9    22 |     5    63   110 |     5    25    78 |
17:33:18 |    148 |    67    41   164 |    64    25   131 |     7     7    17 |     5    57    74 |     5    35    96 |
17:33:22 |    147 |    70    40   106 |    62    26    91 |     4     7    12 |     5    59   105 |     5    28   100 |

TPC-C Benchmark Completed: Runtime 60014 ms,  13669.81 TpmC, 227.83 Tps.
     New-Order -> TX: 6111 (Failed: 0, Retries: 0), Tpmc: 6109.57, Tps: 101.83, Avg-Rt: 29.02 ms, Max-Rt: 164 ms, ofTotal: 44.69 %
       Payment -> TX: 5909 (Failed: 0, Retries: 0), Tpmc: 5907.62, Tps: 98.46, Avg-Rt: 13.35 ms, Max-Rt: 131 ms, ofTotal: 43.22 % (>43.0% is OK)
  Order-Status -> TX: 564 (Failed: 0, Retries: 0), Tpmc: 563.87, Tps: 9.40, Avg-Rt: 4.24 ms, Max-Rt: 22 ms, ofTotal: 4.12 % (> 4.0% is OK)
      Delivery -> TX: 551 (Failed: 0, Retries: 0), Tpmc: 550.87, Tps: 9.18, Avg-Rt: 50.55 ms, Max-Rt: 129 ms, ofTotal: 4.03 % (> 4.0% is OK)
   Stock-Level -> TX: 538 (Failed: 0, Retries: 0), Tpmc: 537.87, Tps: 8.96, Avg-Rt: 12.09 ms, Max-Rt: 100 ms, ofTotal: 3.93 % (> 4.0% is OK)
   
```

### Get Started

You can start TPC-C test by reference "deploy.txt" under deploy directory.

### Build
To build the jar files, you must use minimum version of Java 11 with Apache ant.

```
$ ant clean
$ ant

All build files is under 'dist' directory.

```





