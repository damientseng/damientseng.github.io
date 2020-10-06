---
layout: post
title: "The SQL Rewriting Myth"
author: "Damien Tseng"
categories: Big-Data
tags: [Hive]
mathjax: true
---
<br />
As a data engineer, Hive is one of the most heavily used tools in my day-to-day work. Apart from writing SQL queries, part of my time was spent on reviewing queries written by others. It's delightful to see those pieces of code optimized to the extreme. One prevalent pattern of performance tuning I spotted is rewriting, which is trickier than other ones.  

By _rewriting_, I'm referring to the trick of organizing code in a way that seems to be capable of generating better execution plans than their original intuitive counterparts. Among others, the most famous instance is definitely the `count distinct` rewriting, i.e. breaking the simple `select count(distinct col) from tbl` into the following form with two phases:

```sql
-- SQL1
select
  count(1)
from (
  select col from tbl group by col
) tmp;
```

The frustrating thing is, we don’t always benefit from the efforts of rewriting, while extra complexity is surely introduced to compromise the maintainability of code. Some folks, especially those who have just started coding with Hive SQL, tend to rewrite wherever possible. It's great to realize the importance of efficiency optimization, whereas it's even more important to know what exactly you are doing.


This post reviews some of the most commonly used code rewriting skills. I'll try to explain their assumptions and underlying mechanism, their pitfalls, and the suitable way of using them. Let's get started distinct aggregations.
# Distinct Aggregations
There are basically two categories of algorithms for distinct aggregations, set-based and sorting-based. To make distinct aggregations on massive data possible, Apache Hive primarily implements the sorting-based strategy. When doing distinct aggregations on a large dataset without grouping/partitioning, the parallelism of the reduce-side becomes 1. This may lead to a potentially severe data hot-spot problem. A simple trick to solve the problem is to replace the code with a logically equivalent form, as shown above.


A direct global `count distinct` is rarely seen during my work of code review. Regardless of the data scale and the choice of execution engine, people prefer the `group by` form. Problem? yes, not major though.


One thing I want to point out is that the previous _SQL1_ has a two-phase execution plan, which means one more expensive shuffle is introduced. As a result, the improvement obtained by employing higher parallelism from the extra `group by` or `distinct` could be canceled out by the loss of the other stage.


The ideal way of deciding whether or not to rewrite should be cost-based. If you are using a legacy version of Hive without CBO optimization based on Calcite, run some experiments to find out for sure as to which form to use. While if your installation has CBO in place, make use of it. Here is a checklist:

1. Intuitively write your SQL and let the engine do the optimizations for you.
1. Verify that the rewriting is enabled with `hive.optimize.distinct.rewrite` set to `true`.
1. Make sure CBO is on by checking the properties like `hive.cbo.enable`, `hive.stats.autogather` and `hive.stats.column.autogather`, so that `CalcitePlanner` is functional.
1. Use an execution engine other than MapReduce.

Note, this built-in optimization is applicable for various standard aggregations on `distinct`, not just the `count distinct`, `sum(distinct col)` for example.


Also, it's possible to do the rewriting on queries with distinct aggregations upon multiple columns, with the help of `grouping sets` and the virtual column `grouping__id`. The SQL `select count(distinct ca), count(distinct cb) from tbl` has the following equivalent form:
```sql
-- SQL2
select
  count(if(id=1 and ca is not null, 1, null)),
  count(if(id=2 and cb is not null, 1, null))
from (
  select
    grouping__id as id, ca, cb
  from tbl
  group by ca, cb
  grouping sets(ca, cb)
) tmp
```
Though potentially performant, writing and reading such a lengthy form of code from scratch is overwhelming and error-prone. It's highly recommended to leave such complexity to the system. Luckily, this optimization strategy is implemented by `HiveExpandDistinctAggregatesRule` so this pattern is automatically detected by Hive, making our life a little bit easier.


Some related snippets from the source code of Hive:
```java
//https://github.com/apache/hive/blob/d69aa36c131f50044dc43cdc09a30a7b32e3496a/ql/src/java/org/apache/hadoop/hive/ql/parse/CalcitePlanner.java#L2017
// Run this optimization early, since it is expanding the operator pipeline.
if (!conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("mr") &&
  conf.getBoolVar(HiveConf.ConfVars.HIVEOPTIMIZEDISTINCTREWRITE)) {
// Its not clear, if this rewrite is always performant on MR, since extra map phase
// introduced for 2nd MR job may offset gains of this multi-stage aggregation.
// We need a cost model for MR to enable this on MR.
generatePartialProgram(program, true, HepMatchOrder.TOP_DOWN,
    HiveExpandDistinctAggregatesRule.INSTANCE);
}
```
# Skewed Group-by
A few days ago, I was inquired by an analyst about a SQL query he tried to "optimize". The query was intended to do a grouped aggregation, where the group-by column is skewed. The original intention can be expressed as follows.
```sql
-- SQL3
select ca, sum(cb), count(distinct cc) from tbl group by ca;
```
After spotting the data-skew, an effort was made to rewrite the query to this form:
```sql
-- SQL4
select ca, sum(cb), count(1)
from (
  select ca, cc, sum(cb) as cb from tbl
  group by ca, cc
) tmp
group by ca;
```
It's expected that this code is translated into two jobs. The first job tries to make the column `cc` distinct within each group of `ca`, such that a follow-up job can get a distinct count by just calculating a `count(1)`. But unexpectedly, _SQL4_ is still executed with just one job as if no rewriting was done.  


Well, this is interesting, though not surprising at all. A remarkable part of Hive's built-in efforts to query optimization was to minimize the shuffling cost. One of the efforts tries to remove extra jobs if the data grouped by some keys are later grouped by a subset of these keys. Let’s take _SQL4_ for instance. The subquery which groups data by `ca` and `cc` is later referred to for some aggregations on  `ca` only. So a direct translation of this query has two shuffles. This, of course, has a shorter equivalence with only one shuffle. Hive sees this pattern and "optimizes" the code for you, as if the code is reversed back to _SQL3_, leaving the user confused and frustrated.

Though this mechanism was introduced quite early, since version 0.6.0, it's not that well-known. It can be turned off by setting `hive.optimize.reducededuplication` to `false`. But still, there's a better way for sure.


In fact, skewed group-by is so universally seen that Hive tried to address the problem systematically since as early as version 0.3.0. Hive can automatically do the rewriting for you if the property `hive.groupby.skewindata` is set to `true`. It's recommended to utilize this configuration rather than expanding the query by yourself.

To wrap up, we have two ways of handling skewed group-by. Option 1 is to break the code into two phases and turn off `hive.optimize.reducededuplication`. option 2 is to just switch on `hive.groupby.skewindata`. If you take the former, do make sure `hive.groupby.skewindata` is off, or else you'll end up with an execution plan with **four** stages.

# Grouping Sets
The implementation of grouping sets places a potential performance issue to Hive. Grouping sets means that each row from the source dataset may have an impact on several groups of the resultset. For a grouping sets on D columns, there may be as many as $$2^D$$ ways of combining columns. The way Hive implements this is to make N copies of each row, where N is the number of combinations in `grouping sets(...)`. This may incur major data expansion that potentially slows down the map side tasks, as each task has to take N-1 times more workloads.

Some folks may try to solve this problem by introducing an extra stage before the `grouping sets` expansion happens. So for the following query:
```sql
-- SQL5
select ca, cb, sum(cc) from tbl
group by ca cb
with cube
```
A longer version that's potentially better is like this:
```sql
--SQL6
with cte as (
  select ca, cb, sum(cc) as cc
  from tbl
  group by ca, cb
)
select ca, cb, sum(cc)
from cte
group by ca cb
with cube
```
This way of rewriting tries to reduce the data being handled by the operators of `grouping sets` by doing a pre-aggregation. The introduction of one more stage into the execution plan means that the improvement is not guaranteed. But anyway, it's worth trying.


OLAP cube operations like `grouping sets` are commonly seen patterns of using Hive (`with cube` and `roll up` are just syntax sugars based on `grouping sets`). So system-level supports for such optimizations are expected. That’s right, this functionality has been around for some time. The related configuration property is `hive.new.job.grouping.set.cardinality`, which configures the least number of combinations patterns to enforce an extra pre-aggregating stage. The default value is 30. And to make the decision more eager, you can make it low enough (e.g. 1).
# Predicate Pushdown
Moving data around is one of the most expensive works in distributed data processing systems. If data filtering happens, it should happen as early as possible so that fewer data are transferred through the network. Predicate pushdown is an effort to do exactly this.

A predicate pushdown optimization can be purely rule-based and is deducible with relational algebra. As a result, SQL users can do the trick themselves without much work. Take the following query for instance:
```sql
-- SQL7
select ta.key, ta.val, tb.val
from ta join tb on ta.key = tb.key
where ta.val > 5 and tb.val < 7
```
When a predicate pushdown is explicitly expressed, we can get this:
```sql
-- SQL8
select
  tmpa.key, tmpa.val, tmpb.val
from (
  select key, val from ta where val > 5
) tmpa
join (
  select key, val from tb where val < 7
) tmpb
on tmpa.key = tmpb.key
```
It seems as though now data are filtered before the joining, leaving a smaller dataset for the join.


A little bit of vocabulary before further discussion. There are two kinds of predicates in outer joins, those in the `join` clause and those in the `where` clause. They are usually referred to as during-join predicates and after-join predicates respectively. For a joining operation like `a left join b on ...`, the table to the left side is called the Preserved Row Table, and the right side is called the Null Supplying Table.

Although hive has a pretty mature mechanism for automatic predicate pushdown, some argue that it's not so intuitive to reason and thus error-prone (are after-join predicates on the Null Supplying Table pushable in left-joins?). Consequently, the safer choice would be to explicitly write pushdowns wherever applicable. Have to admit that a part of me is with this argument. But I do have realized that it's relatively easy to grasp the rule after working with some queries. Here are some tips for a better understanding of the predicate pushdown mechanism.  

Inner join is a simplistic case. It's safe to conclude that all predicates can be pushed, no matter where they are. The reasoning is straightforward. The two tables function equally with each other, and filtering on the source dataset yields identical results to filtering on the output of the join.  


For a left join, things are more complicated. The primary intention of after-join predicates is to filter on the results of the join. An after-join predicate on the Preserved Row Table is pushed, as we can verify that rows in the Preserved Row Table which pass the filtering conditions are correctly placed in the final result set even if the filtering happens before the join. While for an after-join predicate on the Null Supplying Table, if it's pushed down, it usually results in a larger (thus wrong) dataset. So an after-join predicate on the Null Supplying Table is **not** pushed.  

The Null Supplying Table supplies a null when no matching row is found that satisfies all the joining conditions. If some of the conditions are merely predicated on the Null Supplying Table, there are two equivalent ways to check them. We can take the whole dataset of the Null Supplying Table and check the predicates while streaming through the Preserved Row Table. Or we can do the checking before the join. As a result, during-join predicates upon the Null Supplying Table can be pushed. As for during-join predicates for the Preserved Row Table, it's straightforward to see that a pushdown prematurely reduces data, so they should not be pushed. We can make symmetric conclusions about right joins.  

There is no Null Supplying Table in full outer joins, all the tables are Preserved Row Tables. Yet the same rules are applicable here. The conclusion can be simplified: after-join predicates on the Preserved Row Table can be pushed.  

The system-level PPD optimization is configurable through `hive.optimize.ppd`. Making use of this feature may significantly reduce the size of your code. But anyway, a rule of thumb here, is to explicitly push the predicates whenever you don’t feel confident about your reasoning.  
# The Takeaway
So many years after its open-sourcing, Apache Hive is still evolving. New features are being introduced and old features are being revised. Along with the evolution of the tool, the _best practices_ of using it evolves too. It's important to keep our knowledge up-to-date so that nice updates are not missed out. After all, one _best practice_ that's profoundly true is to favor systematic optimizations over user code level crafting.  


The importance of code readability is something that's often underestimated. Sometimes _Big Data Engineers_, emphasize too much on the _Big_ part while ignoring the _Engineers_ part. Here I'm quoting a famous saying from Donald Knuth:
> ... premature optimization is the root of all evil (or at least most of it) in programming.

Query rewriting usually compromises the readability, sometimes for nothing. The general idea about premature optimization holds in the context of big data development too. To wit, don’t over exaggerate the scale of your data, and favor code readability over minor performance improvement.
