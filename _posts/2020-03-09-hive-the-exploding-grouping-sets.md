---
layout: post
title: "The Exploding Grouping Sets"
author: "Damien Tseng"
categories: Big-Data
tags: [Hive]
mathjax: true
---
<br />
Our data warehouse was built on Hive. One of the common use cases is running `grouping sets`-like queries to generate multi-dimensional data cubes. Recently we got some feedbacks complaining about the performance: analyst claiming that it takes forever even for the most straightforward queries containing `with cube` to finish.  

Let's take a simplified case for example. There is a take `mdb.dw_tb_d` with three columns: `ca` , `cb` , `uid` . And we try to calculate a cube with `ca` and `cb` as dimensions, and `count(*)` and `count(distinct uid)` as measures. Using HQL, it's expressed as follows:

```sql
-- SQL1
select
    if(cast(GROUPING__ID as tinyint)&1=0, 'ALL', ca)  as ca
    ,if(cast(GROUPING__ID as tinyint)&2=0, 'ALL', cb) as cb
    ,count(1)             as pv
    ,count(distinct uid)  as uv
from mdb.dw_tb_d
group by ca, cb
with cube
;
```
# Potential Issues
As simple as *SQL1* is, it shows the business intention in an intuitive manner. Most of the time, it works like a charm. But sometimes it doesn’t: the running time might be ridiculously long. To understand the issue, we may need a little bit of knowledge about how grouping sets is implemented in Hive.  

To generate multi-dimensional results, it means that every row in the original table should have some impacts on multiple rows in the result set. Say we have *N* dimensions, there will be $$k=2^N$$ ways to combine these dimensions, and each row contributes to every kind of combination. To achieve this, Hive makes *k* copies of each row at the map side and attaches a tag named `GROUPING__ID` along with these copies. Assuming $$N=3$$, there are 8 different `GROUPING__ID`s whose binary format range from `0b000` to `0b111`. The three positions correspond to the states of the three dimensions: whether they are kept as a detailed value or aggregated in a particular group. Note that `with cube` and `rollup` are just syntax sugars, they are translated into `grouping sets`  before execution. Since there may be a huge number of copies when *N* is large, it's mandatory to turn on map-side aggregation (there is a [patch](https://issues.apache.org/jira/browse/HIVE-3508) that removes this restriction). Map-side aggregation is implemented with hash maps, when data explode, each map task may have a huge workload, thus slowing down the execution.  

Let's revisit *SQL1*. It can be translated into grouping-sets style as `((ca, cb), (ca), (cb), ())`. The last group has no dimension at all, which leads to global aggregation on the whole data set. This means that the partitioning key of this group is identical: *null*. So there is a single reduce task that needs to process all the data in the table `mdb.dw_d`. It's bad news for the cardinality aggregation `count(distinct uid)`: skewed workload fails to utilize parallelism and severely hinders the query from parallel execution. Although data skew may happen to other kinds of dimension combinations, the takeaway is it happens.

The next question is, how can we tune *SQL1* for better performance?
# Cool Down
When seeing a reduce phase stuck as 99% for a long time, an experienced user should realize that there's data skew at the reduce side. Compared with other issues, this one is easier to spot. And the cure to it is broadly addressed.  

First, we can try to take the `with cube` apart, and optimize the `count distinct` aggregation by a sub-query. `count distinct` has two logically equivalent expressions:
```sql
select count(distinct uid) from tb;
```
```sql
select count(1) from (select uid from tb group by uid) tmp;
```

This is a quite well-known way of dealing with data hot spot, which can be formalized as **salting -> partitioning -> pre-aggregate -> re-aggregate.** Now with this strategy in mind, we can rewrite *SQL1* into *SQL2*:
```sql
-- SQL2
set hive.exec.parallel = true;

select
    if(cast(GROUPING__ID as tinyint)&1=0, 'ALL', ca)  as ca
    ,if(cast(GROUPING__ID as tinyint)&2=0, 'ALL', cb) as cb
    ,count(1)            as pv
    ,count(distinct uid) as uv
from mdb.dw_tb_d
group by ca, cb
grouping sets((ca, cb), (ca), (cb))

union all
select
    'ALL'  as ca
    ,'ALL' as cb
    ,sum(pv)  as pv
    ,count(1) as uv
from (
    select
        uid
        ,count(1) as pv
    from mdb.dw_tb_d
    group by uid
) tmp
;
```

The grouping with no dimension is explicitly expressed, and it's combined with other groups by `union all`. Of course, we don’t wanna end up running two jobs without dependency consecutively, so we turn on parallel execution by setting `set hive.exec.parallel = true;`

As mentioned above, data skew may happen to any kind of group. As such, this kind of decomposing and rewriting can be taken to the extreme, leading to *SQL3*. This strategy not only avoids data skew but also speeds up by utilizing higher parallelism(though it's way too verbose, we'll come back at this later). One important thing to note: the performance improvement is not 100% guaranteed, as we are introducing one more stage into each sub-query which costs one more expensive shuffle phase.
```sql
-- SQL3
set hive.exec.parallel = true;

select
    'ALL'  as ca
    ,'ALL' as cb
    ,sum(pv)  as pv
    ,count(1) as uv
from (
    select
        uid
        ,count(1) as pv
    from mdb.dw_tb_d
    group by uid
) tmp1

union all
select
    ca
    ,'ALL' as cb
    ,sum(pv)  as pv
    ,count(1) as uv
from (
    select
        uid
  			,ca
        ,count(1) as pv
    from mdb.dw_tb_d
    group by uid, ca
) tmp2
group by ca

union all
select
    'ALL' as ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from (
    select
        uid
        ,cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by uid, cb
) tmp3
group by cb

union all
select
    ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from (
    select
        uid
        ,ca
        ,cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by uid, ca, cb
) tmp4
group by ca, cb
;
```


# Make it Succinct
The issue with *SQL3* is obvious: the code grows with the number of combinations. It's tiring(though possible) to write and maintain such kind of code. So we have to make the code shorter and more concise. We can achieve this by yet another rewriting:
```sql
-- SQL4
with cte as (
    select
        uid
        ,if(GROUPING__ID in ('4', '6'), 'ALL', ca) as ca
        ,if(GROUPING__ID in ('4', '5'), 'ALL', cb) as cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by ca, cb, uid
    grouping sets((uid), (uid, ca), (uid, cb), (uid, ca, cb))
)
select
    ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from cte
group by ca, cb
;
```
```sql
Stage-Stage-1: Map: 4  Reduce: 2
Stage-Stage-2: Map: 2  Reduce: 1
...
Time taken: 656.265 seconds, Fetched: 352 row(s)
```

With *SQL4*, the multiple map side table scannings are merged by `grouping set`. Its performance is downgraded from compered with *SQL3*, but it's succinct and thus readable. Another way of organizing code like *SQL* is to make use of `GROUPING__ID`, just as how it is designed to work within Hive:
```sql
-- SQL5
with cte1 as (
    select
        uid
        ,GROUPING__ID as gid
        ,ca
        ,cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by ca, cb, uid
    grouping sets((uid), (uid, ca), (uid, cb), (uid, ca, cb))
)
select
    if(gid in ('4', '6'), 'ALL', ca)  as ca
    ,if(gid in ('4', '5'), 'ALL', cb) as cb
    ,sum(pv)  as pv
    ,count(1) as uv
from cte1
group by gid, ca, cb;
```

Now under the framework of *SQL4*, we try some other ways of tuning.  

# Expansion
Is there a way to alleviate the data explosion at the map-side? Well, there is.

Among others, the most straightforward one is to use more mappers. The number of mappers is not directly configurable, but mainly decided by the number of data splits to process. If the storage format of the input table is splittable (for example, TextFile without compression), then we are free to adjust the split size to control the number of mappers at our will. But if the file format is not randomly  splittable like ORC, then the number of mappers is impacted by the granularity of the smallest splittable unit, like a stripe in ORC files.

Apart from that, the varying implementations of `InputFormat` behave differently. Small files may be combined together to generate a larger split. Before version 0.6, `hive.input.format` was defaulted to `CombineHiveInputFormat`. We can try to avoid file combination by setting it to `HiveInputFormat`. Moreover, we can set a lower value of  `mapreduce.input.fileinputformat.split.maxsize`  to avoid stripe merging on ORC files.

Our *SQL6* only adds some configurations upon *SQL4* to control the generation of splits:
```sql
-- SQL6
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=10000;
set mapreduce.input.fileinputformat.split.minsize.per.node=10000;
set mapreduce.input.fileinputformat.split.minsize.per.rack=10000;

with cte as (
    select
        uid
        ,if(GROUPING__ID in ('4', '6'), 'ALL', ca) as ca
        ,if(GROUPING__ID in ('4', '5'), 'ALL', cb) as cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by ca, cb, uid
    grouping sets((uid), (uid, ca), (uid, cb), (uid, ca, cb))
)
select
    ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from cte
group by ca, cb
;
```

When `orc.stripe.size` is 256M:
```
Stage-Stage-1: Map: 4  Reduce: 2
Stage-Stage-2: Map: 2  Reduce: 1
...
Time taken: 399.946 seconds, Fetched: 352 row(s)
```


And if we configure a lower `orc.stripe.size` (and rewrite the table):
```sql
alter table mdb.dw_tb_d set TBLPROPERTIES('orc.stripe.size'='67108864');
```


We see another improvement of the map-side parallelism:
```sql
Stage-Stage-1: Map: 24  Reduce: 3
Stage-Stage-2: Map: 3  Reduce: 1
...
Time taken: 324.412 seconds, Fetched: 352 row(s)
```


Chances are, those tables are produced by another team, and as sole consumers we don't have control over the number of mappers. Even if this is the case, we can still achieve optimal performance at our side of the code. The trick is to add one stage before the data explosion happens. By explicitly configuring the number of reducers, we get to decide how many files the original data are split into. Besides, the intermediate data can be of TextFile without compression. Consequently, we get to control the number of map tasks for `grouping sets`'s data explosion.

Let's add one stage and do some aggregation first:
```sql
-- SQL7
with pre1 as (
    select
        uid
        ,ca
        ,cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by ca, cb, uid
)
,pre2 as (
    select
        uid
        ,if(GROUPING__ID in ('4', '6'), 'ALL', ca) as ca
        ,if(GROUPING__ID in ('4', '5'), 'ALL', cb) as cb
        ,sum(pv) as pv
    from pre1
    group by ca, cb, uid
    grouping sets((uid), (uid, a), (uid, ca), (uid, ca, cb))
)
select
    ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from pre2
group by ca, cb
;
```

*SQL7*'s performance is summarized as this:
```sql
Stage-Stage-1: Map: 2  Reduce: 2
Stage-Stage-2: Map: 36  Reduce: 20
Stage-Stage-3: Map: 1  Reduce: 1
...
Time taken: 440.337 seconds, Fetched: 352 row(s)
```
Although Stage-1 has only 2 reducers -- thus generating at most 2 files, the subsequent Stage-2 processes uncompressed TextFile at significantly higher parallelism. Now we have 36 mappers for data explosion.  

In fact, Hive has a built-in strategy optimization that does exactly this. When the number of combination groups is greater than `hive.new.job.grouping.set.cardinality`(defaults to 30), an additional group of MR is introduced. It aggregates on all the columns specified in the `group by` clause. The drawback is that it does not support explicit `count distinct`. But anyway, we can get rid of the first CTE in *SQL7*. Now we have *SQL8*, which is almost identical to *SQL4* except a configuration.
```sql
-- SQL8
set hive.new.job.grouping.set.cardinality=3;

with cte as (
    select
        uid
        ,if(GROUPING__ID in ('4', '6'), 'ALL', ca) as ca
        ,if(GROUPING__ID in ('4', '5'), 'ALL', cb) as cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by ca, cb, uid
    grouping sets((uid), (uid, ca), (uid, cb), (uid, ca, cb))
)
select
    ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from cte
group by ca, cb
;
```

The performance is similar to that of *SQL7*.
```sql
Stage-Stage-1: Map: 2  Reduce: 2
Stage-Stage-2: Map: 44  Reduce: 23
Stage-Stage-3: Map: 1  Reduce: 1
...
Time taken: 440.207 seconds, Fetched: 352 row(s)
```

Also, recall that the parallelism of reduce-side is explicitly configurable:
```sql
-- SQL9
set hive.new.job.grouping.set.cardinality=3;
set mapred.reduce.tasks = 16;

with cte as (
    select
        uid
        ,if(GROUPING__ID in ('4', '6'), 'ALL', ca) as ca
        ,if(GROUPING__ID in ('4', '5'), 'ALL', cb) as cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by ca, cb, uid
    grouping sets((uid), (uid, ca), (uid, cb), (uid, ca, cb))
)
select
    ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from cte
group by ca, cb
;
```


```sql
Stage-Stage-1: Map: 2  Reduce: 16
Stage-Stage-2: Map: 39  Reduce: 16
Stage-Stage-3: Map: 1  Reduce: 16
...
Time taken: 392.228 seconds, Fetched: 352 row(s)
```
Stage-1 now runs fast since it has higher parallelism. Note that Stage-3's reduce also runs under higher parallelism. This is not a problem if the query has no `insert` operation. But if it does, small-file problem may occur.  

Fortunately, we have yet another way to control the number of reduce tasks. By tuning `hive.exec.reducers.bytes.per.reducer`, we can control the workload of a single reducer, and implicitly control the parallelism of the reducers.
```sql
-- SQL10
set hive.new.job.grouping.set.cardinality=3;
set hive.exec.reducers.bytes.per.reducer = 16000000; -- defaults: 256*1000*1000

with cte as (
    select
        uid
        ,if(GROUPING__ID in ('4', '6'), 'ALL', ca) as ca
        ,if(GROUPING__ID in ('4', '5'), 'ALL', cb) as cb
        ,count(1) as pv
    from mdb.dw_tb_d
    group by ca, cb, uid
    grouping sets((uid), (uid, ca), (uid, cb), (uid, ca, cb))
)
select
    ca
    ,cb
    ,sum(pv)  as pv
    ,count(1) as uv
from cte
group by ca, cb
;
```
```sql
Stage-Stage-1: Map: 2  Reduce: 23
Stage-Stage-2: Map: 43  Reduce: 78
Stage-Stage-3: Map: 1  Reduce: 1
...
Time taken: 388.211 seconds, Fetched: 352 row(s)
```

This way, the performance of Stage-1 is optimal, and we don’t have the risk of generating too many small files when there is an `insert`. Compared with the origin table, the result set is small, so the workload of Stage-3 is quite low.
# Summary
In the article, we tried to address the problem of data explosion when using Hive with `grouping sets`. The intro describes the problem with a simple example, which offers *SQL1* as a starting point of tuning. The rest of this writing takes several steps to solve this problem.  

By decomposition and rewriting, we arrive at *SQL3*, which has the optimal time efficiency, but it's not maintainable. By trading efficiency for readability, *SQL4* is achieved. Later efforts are devoted to handling data explosion with higher parallelism while avoiding other problems like small files. Finally, we get to *SQL9* and *SQL10*, which attains an elegant balance between performance and maintainability/readability.
