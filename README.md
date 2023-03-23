

Scenario Based questions:

Will the reducer work or not if you use “Limit 1” in any HiveQL query?
Ans. It will work for aggregation type of queries. and it will not work simple/direct queries.


Suppose I have installed Apache Hive on top of my Hadoop cluster using default metastore configuration. Then, what will happen if we have multiple clients trying to access Hive at the same time? 
Ans. It will not allow to access other client In metastore configuration only one system has access because it will allow one system at one point of time in standalone mode.
In computation hive engine was seperately configured. every system has own jdbc/odbc connector to connect with hive engine.


Suppose, I create a table that contains details of all the transactions done by the customers: CREATE TABLE transaction_details (cust_id INT, amount FLOAT, month STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ;
Now, after inserting 50,000 records in this table, I want to know the total revenue generated for each month. But, Hive is taking too much time in processing this query. How will you solve this problem and list the steps that I will be taking in order to do so?

We can create table with partitioned like 
CREATE TABLE pratitioned_transaction_details (cust_id INT, amount FLOAT, month STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ ;

Set hive.exec.dynamic.partition.mode=nonstrict;

then load the data what ever we need
insert overwrite table pratitioned_transaction_details(month) partition(month) select cusT_id,sum(amount) as total revenue,month,country from transaction_details;


How can you add a new partition for the month December in the above partitioned table?

using alter we can add new partition
alter table pratitioned_transaction_details add partition(month="dec") location '\location of table';



I am inserting data into a table based on partitions dynamically. But, I received an error – FAILED ERROR IN SEMANTIC ANALYSIS: Dynamic partition strict mode requires at least one static partition column. How will you remove this error?

Enable the dynamic partition
set hive.exec.dynamic.partition=true;
Set hive.exec.dynamic.partition.mode=nonstrict;


suppose, I have a CSV file – ‘sample.csv’ present in ‘/temp’ directory with the following entries:
id first_name last_name email gender ip_address
How will you consume this CSV file into the Hive warehouse using built-in SerDe?

CREATE TABLE emp_details (id INT,first_name string,last_name string,email string,gender string ,ipaddress string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile;



Suppose, I have a lot of small CSV files present in the input directory in HDFS and I want to create a single Hive table corresponding to these files. The data in these files are in the format: {id, name, e-mail, country}. Now, as we know, Hadoop performance degrades when we use lots of small files.
So, how will you solve this problem where we want to create a single Hive table for lots of small files without degrading the performance of the system?

normal text file e cannot append the data in hive.
Sequence file may append the data.



Hive Practical questions:

Hive Join operations

Create a  table named CUSTOMERS(ID | NAME | AGE | ADDRESS   | SALARY)
Create a Second  table ORDER(OID | DATE | CUSTOMER_ID | AMOUNT
)

Now perform different joins operations on top of these tables
(Inner JOIN, LEFT OUTER JOIN ,RIGHT OUTER JOIN ,FULL OUTER JOIN)



hive> create table customers(
    > id int,
    > name string,
    > age int,
    > address string,
    > salary float
    > )
    > row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    > with serdeproperties (
    > "seperatorChar" = ",",
    > "quoteChar" = "\'",
    > "escapeChar" = "\\"
    > )
    > stored as textfile;
OK
Time taken: 0.115 seconds
hive> describe customers;
OK
id                      string                  from deserializer
name                    string                  from deserializer
age                     string                  from deserializer
address                 string                  from deserializer
salary                  string                  from deserializer

hive> load data local inpath '/home/cloudera/Downloads/hivepractical/assignments/customer.csv' into table customers;
Loading data to table assignment.customers
Table assignment.customers stats: [numFiles=1, totalSize=176]
OK
Time taken: 0.346 seconds
hive> select * from customers;
OK
1       RAM     21      JNROAD,UP       10000
2       KUMAR   22      HIGHRAD,AP      100000
3       RAJ     20      MAINROAD,MP     1000
4       JOHN    29      OUTERRINGROAD,TS        20000
5       KO      27      JNROAD,UP       15000
10      LO      30      NOIDA,DELHI     20000
Time taken: 0.07 seconds, Fetched: 6 row(s)

hive> describe orders;
OK
id                      int
date                    string
c_id                    int
amount                  float
Time taken: 0.081 seconds, Fetched: 4 row(s)
hive> load data local inpath '/home/cloudera/Downloads/hivepractical/assignments/orders.csv' overwrite into table orders;
Loading data to table assignment.orders
Table assignment.orders stats: [numFiles=1, numRows=0, totalSize=96, rawDataSize=0]
OK
Time taken: 0.399 seconds
hive> select * from orders;
OK
1001    12/12/2001      1       20000.0
1002    12/08/2014      2       30000.0
1003    11/03/2021      3       40000.0
1004    11/09/2022      7       50000.0
Time taken: 0.069 seconds, Fetched: 4 row(s)

hive> create table buck_orders(
    > id int,
    > date string,
    > c_id int,
    > amount float
    > )
    > clustered by (c_id)
    > sorted by (c_id)
    > into 2 buckets;
OK
Time taken: 0.119 seconds
hive> describe formatted buck_orders;
# Storage Information
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:            org.apache.hadoop.mapred.TextInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
Compressed:             No
Num Buckets:            2
Bucket Columns:         [c_id]
Sort Columns:           [Order(col:c_id, order:1)]
Storage Desc Params:
        serialization.format    1
Time taken: 0.085 seconds, Fetched: 35 row(s)

hive> create table buck_customers(
    > id int,
    > name string,
    > age int,
    > address string,
    > salary float
    > )
    > clustered by (id)
    > sorted by (id)
    > into 2 buckets;
OK
Time taken: 0.116 seconds
hive> describe formatted buck_customers;
# Storage Information
SerDe Library:          org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
InputFormat:            org.apache.hadoop.mapred.TextInputFormat
OutputFormat:           org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
Compressed:             No
Num Buckets:            2
Bucket Columns:         [id]
Sort Columns:           [Order(col:id, order:1)]
Storage Desc Params:
        serialization.format    1
Time taken: 0.085 seconds, Fetched: 31 row(s)



inner join: for(map join)
hive> select * from customers c inner join orders o on c.id = o.c_id;
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
2022-09-20 00:06:29,555 Stage-3 map = 0%,  reduce = 0%
2022-09-20 00:06:39,232 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 3.81 sec
MapReduce Total cumulative CPU time: 3 seconds 810 msec
Ended Job = job_1663640784885_0005
MapReduce Jobs Launched:
Stage-Stage-3: Map: 1   Cumulative CPU: 3.81 sec   HDFS Read: 8091 HDFS Write: 158 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 810 msec
OK
1       RAM     21      JNROAD,UP       10000   1001    12/12/2001      1       20000.0
2       KUMAR   22      HIGHRAD,AP      100000  1002    12/08/2014      2       30000.0
3       RAJ     20      MAINROAD,MP     1000    1003    11/03/2021      3       40000.0
Time taken: 27.411 seconds, Fetched: 3 row(s)

Left outer join(for bucket join)
hive> set hive.optimize.bucketmapjoin=true;
hive> select * from customers c left outer join orders o on c.id = o.c_id;
Query ID = cloudera_20220920001111_d3eca109-c80f-45f6-ac81-b93b26fa7403
Total jobs = 1
Execution log at: /tmp/cloudera/cloudera_20220920001111_d3eca109-c80f-45f6-ac81-b93b26fa7403.log
2022-09-20 12:11:14     Starting to launch local task to process map join;      maximum memory = 932184064
2022-09-20 12:11:16     Dump the side-table for tag: 1 with group count: 4 into file: file:/tmp/cloudera/a2e5449f-9d29-48d2-87b9-90cc1fda5ab5/hive_2022-09-20_00-11-07_909_4314964738024416816-1/-local-10003/HashTable-Stage-3/MapJoin-mapfile21--.hashtable
2022-09-20 12:11:16     Uploaded 1 File to: file:/tmp/cloudera/a2e5449f-9d29-48d2-87b9-90cc1fda5ab5/hive_2022-09-20_00-11-07_909_4314964738024416816-1/-local-10003/HashTable-Stage-3/MapJoin-mapfile21--.hashtable (440 bytes)
2022-09-20 12:11:16     End of local task; Time Taken: 1.752 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1663640784885_0006, Tracking URL = http://quickstart.cloudera:8088/proxy/application_1663640784885_0006/
Kill Command = /usr/lib/hadoop/bin/hadoop job  -kill job_1663640784885_0006
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
2022-09-20 00:11:24,244 Stage-3 map = 0%,  reduce = 0%
2022-09-20 00:11:31,740 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 2.62 sec
MapReduce Total cumulative CPU time: 2 seconds 620 msec
Ended Job = job_1663640784885_0006
MapReduce Jobs Launched:
Stage-Stage-3: Map: 1   Cumulative CPU: 2.62 sec   HDFS Read: 7962 HDFS Write: 278 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 620 msec
OK
1       RAM     21      JNROAD,UP       10000   1001    12/12/2001      1       20000.0
2       KUMAR   22      HIGHRAD,AP      100000  1002    12/08/2014      2       30000.0
3       RAJ     20      MAINROAD,MP     1000    1003    11/03/2021      3       40000.0
4       JOHN    29      OUTERRINGROAD,TS        20000   NULL    NULL    NULL    NULL
5       KO      27      JNROAD,UP       15000   NULL    NULL    NULL    NULL
10      LO      30      NOIDA,DELHI     20000   NULL    NULL    NULL    NULL



For right outer join:
select * from customers c right outer join orders o on c.id = o.c_id;
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
2022-09-20 00:13:25,107 Stage-3 map = 0%,  reduce = 0%
2022-09-20 00:13:32,456 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 2.48 sec
MapReduce Total cumulative CPU time: 2 seconds 480 msec
Ended Job = job_1663640784885_0007
MapReduce Jobs Launched:
Stage-Stage-3: Map: 1   Cumulative CPU: 2.48 sec   HDFS Read: 7882 HDFS Write: 199 SUCCESS
Total MapReduce CPU Time Spent: 2 seconds 480 msec
OK
1       RAM     21      JNROAD,UP       10000   1001    12/12/2001      1       20000.0
2       KUMAR   22      HIGHRAD,AP      100000  1002    12/08/2014      2       30000.0
3       RAJ     20      MAINROAD,MP     1000    1003    11/03/2021      3       40000.0
NULL    NULL    NULL    NULL    NULL    1004    11/09/2022      7       50000.0
Time taken: 22.502 seconds, Fetched: 4 row(s)

Full outer join:

hive> set hive.enforce.sortmergebucketmapjoin=false;
hive> set hive.auto.convert.sortmerge.join=true;
hive> set hive.optimize.bucketmapjoin = true;
hive> set hive.optimize.bucketmapjoin.sortedmerge = true;
hive> SET hive.auto.convert.join=false;
hive> select * from customers c full outer join orders o on c.id = o.c_id;

Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
2022-09-20 00:16:46,001 Stage-1 map = 0%,  reduce = 0%
2022-09-20 00:17:04,716 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 18.11 sec
2022-09-20 00:17:13,090 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 19.85 sec
MapReduce Total cumulative CPU time: 19 seconds 850 msec
Ended Job = job_1663640784885_0008
MapReduce Jobs Launched:
Stage-Stage-1: Map: 2  Reduce: 1   Cumulative CPU: 19.85 sec   HDFS Read: 16642 HDFS Write: 319 SUCCESS
Total MapReduce CPU Time Spent: 19 seconds 850 msec
OK
1       RAM     21      JNROAD,UP       10000   1001    12/12/2001      1       20000.0
2       KUMAR   22      HIGHRAD,AP      100000  1002    12/08/2014      2       30000.0
3       RAJ     20      MAINROAD,MP     1000    1003    11/03/2021      3       40000.0
4       JOHN    29      OUTERRINGROAD,TS        20000   NULL    NULL    NULL    NULL
5       KO      27      JNROAD,UP       15000   NULL    NULL    NULL    NULL
NULL    NULL    NULL    NULL    NULL    1004    11/09/2022      7       50000.0
10      LO      30      NOIDA,DELHI     20000   NULL    NULL    NULL    NULL







Download a data from the given location - 
https://archive.ics.uci.edu/ml/machine-learning-databases/00360/

Create a hive table as per given schema in your dataset
create table hive_pract2(
date string,
time string,
co_gt float,
pt08_s1_co int,
nmhc_gt float,
c6h6_gt int,
pt08_s2_nmhc int,
nox_gt int,
pt08_s3_nox int,
no2_gt int,
pt08_s4_no2 int,
pt08_s5_o3 int,
t float,
rh float,
ah float
)
row format delimited
field terminated by ','
tblproperties("skip.header.line.count"="1");

 describe hive_pract2;
OK
date                    string
time                    string
co_gt                   float
pt08_s1_co              int
nmhc_gt                 float
c6h6_gt                 int
pt08_s2_nmhc            int
nox_gt                  int
pt08_s3_nox             int
no2_gt                  int
pt08_s4_no2             int
pt08_s5_o3              int
t                       float
rh                      float
ah                      float
Time taken: 0.136 seconds, Fetched: 15 row(s)
hive>





try to place a data into table location
load data local inpath '/Downloads/hivepractical/assignments/data4.csv' overwrite into table hive_pract2;

Perform a select operation .
hive> select * from hive_pract2 limit 10;
OK
hive_pract2.date        hive_pract2.time        hive_pract2.co_gt       hive_pract2.pt08_s1_co  hive_pract2.nmhc_gt     hive_pract2.c6h6_gt     hive_pract2.pt08_s2_nmhc        hive_pract2.nox_gt   hive_pract2.pt08_s3_nox hive_pract2.no2_gt      hive_pract2.pt08_s4_no2 hive_pract2.pt08_s5_o3  hive_pract2.t   hive_pract2.rh  hive_pract2.ah
10-03-2004      18:00:00        2.6     1360    150.0   11      1046    166     1056    113     1692    1268    13.6    48.9    0.7578
10-03-2004      19:00:00        2.0     1292    112.0   9       955     103     1174    92      1559    972     13.3    47.7    0.7255
10-03-2004      20:00:00        2.2     1402    88.0    9       939     131     1140    114     1555    1074    11.9    54.0    0.7502
10-03-2004      21:00:00        2.2     1376    80.0    9       948     172     1092    122     1584    1203    11.0    60.0    0.7867
10-03-2004      22:00:00        1.6     1272    51.0    6       836     131     1205    116     1490    1110    11.2    59.6    0.7888
10-03-2004      23:00:00        1.2     1197    38.0    4       750     89      1337    96      1393    949     11.2    59.2    0.7848
11-03-2004      00:00:00        1.2     1185    31.0    3       690     62      1462    77      1333    733     11.3    56.8    0.7603
11-03-2004      01:00:00        1.0     1136    31.0    3       672     62      1453    76      1333    730     10.7    60.0    0.7702
11-03-2004      02:00:00        0.9     1094    24.0    2       609     45      1579    60      1276    620     10.7    59.7    0.7648
11-03-2004      03:00:00        0.6     1010    19.0    1       561     -200    1705    -200    1235    501     10.3    60.2    0.7517
Time taken: 0.082 seconds, Fetched: 10 row(s)

Perform group by operation .
 select date,sum(c6h6_gt) as total_c6h6 from hive_pract2 group by (date) limit 5;
 MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.18 sec   HDFS Read: 756398 HDFS Write: 5875 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 180 msec
OK
01-01-2005      161
01-02-2005      276
01-03-2005      62
01-04-2004      -358
01-04-2005      72
..........

Perform filter operation kinds of filter examples .

 select c6h6_gt from hive_pract2 where date = '31-08-2004';
OK
5
4
2
2
2
4
6

show and example of regex operation
hive> select regexp_replace('in!eu%ro-n+','[^a-z]','');
OK
ineuron

alter table operation

hive> create table table_name(
    > id int,name string,
    > city string)
    > stored as textfile;
OK
Time taken: 0.366 seconds
hive> alter table table_name rename to sample;
OK
Time taken: 0.186 seconds
hive> describe sample;
OK
id                      int
name                    string
city                    string
Time taken: 0.117 seconds, Fetched: 3 row(s)

drop table operation
hive> drop table sample;
OK
Time taken: 0.336 seconds
hive> describe sample;
FAILED: SemanticException [Error 10001]: Table not found sample
hive>



order by operation . 
select date,sum(c6h6_gt) as total_c6h6 from hive_pract2 group by (date) order by total_c6h6;
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.86 sec   HDFS Read: 755486 HDFS Write: 12032 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 3.42 sec   HDFS Read: 16962 HDFS Write: 5875 SUCCESS
Total MapReduce CPU Time Spent: 8 seconds 280 msec
OK
10-02-2005      -4800
20-06-2004      -4800
16-12-2004      -4800
15-12-2004      -4800
27-08-2004      -4800
09-02-2005      -4800
03-01-2005      -4800
04-01-2005      -4800
09-04-2004      -4592
11-02-2005      -4159
17-12-2004      -3958
26-08-2004      -3581
08-09-2004      -3267
19-06-2004      -1909
26-05-2004      -1557
28-01-2005      -1339
08-02-2005      -1322

where clause operations you have to perform .
select c6h6_gt from hive_pract2 where date = '31-08-2004';
5
4
2
2
2
4
6
21
26
16
15
16
14
12

sorting operation you have to perform .
> select c6h6_gt from hive_pract2 sort by c6h6_gt;
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 8.96 sec   HDFS Read: 753983 HDFS Write: 191 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 5.6 sec   HDFS Read: 4603 HDFS Write: 25 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 560 msec
OK
-200
-200
-200
-200
-200



distinct operation you have to perform .
select distinct(c6h6_gt) from hive_pract2 limit 10;

Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 3.85 sec   HDFS Read: 755872 HDFS Write: 23 SUCCESS
Total MapReduce CPU Time Spent: 3 seconds 850 msec
OK
-200
0
1
2
3
4
5
6
7
8
Time taken: 30.008 seconds, Fetched: 10 row(s)




like an operation you have to perform .
 select date from hive_pract2 where date like '24%' limit 10;
OK
24-03-2004
24-03-2004
24-03-2004
24-03-2004
24-03-2004
24-03-2004
24-03-2004
24-03-2004
24-03-2004
24-03-2004
Time taken: 0.097 seconds, Fetched: 10 row(s)


union operation you have to perform . 
hive> select c6h6_gt from hive_pract2
    > union all
    > select c6h6_gt from hive_pract2 limit 10;
2
2
2
1
1
1
2
4
3
5
5
3
4
5
6
6

18 . table view operation you have to perform .
Time taken: 50.435 seconds, Fetched: 9367 row(s)
hive> Create view order_view as select * from buck_orders;
OK
Time taken: 0.605 seconds
hive> select * from order_view;
OK
1001    12/12/2001      1       20000.0
1002    12/08/2014      2       30000.0
1003    11/03/2021      3       40000.0
1004    11/09/2022      7       50000.0
Time taken: 0.111 seconds, Fetched: 4 row(s)
hive>

