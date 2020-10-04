---
layout: post
title: "The Kind of UDAF You've Never Seen"
author: "Damien Tseng"
categories: Big-Data
tags: [Hive]
mathjax: true
---
<br />
UDF is one of the sources of Apache Hive's strength. This post illustrates a rather novel way of utilizing the interface of Hive UDF. You may already have the experience of developing Hive UDF/UDAF on your own. My story is here to help deepen your understanding of this functionality. I'll demonstrate a novel application of Hive UDAF. Here, I'm overloading the term UDAF. The letter _A_ refers to _Analytics_, not the usual _Aggregate_.


Let's start with an outline of the problem I tried to address. We have various kinds of logs produced by the same individual (a device/ an organization) from different applications. At the time, it's impossible to track these logs with universal keys (session ids/ trace ids). There are some use cases where we need to combine(join) two kinds of logs with the absence of an explicit key. Say we have two tables: a table named `dw_login` containing records about the login time and channel, and a `dw_pay` table about the payments. They have the following structures:  
```txt
dw_login:
login_dt  string
uid       string
channel   string
```
```txt
dw_pay:
pay_dt    string
uid       string
order_id  string  
amt       double
```
We need to combine these two tables such that for each row in `dw_pay`, we have a column named `channel` whose content is the most recent channel from which the `uid` is logged in. And the columns of the output table should be like this:
```txt
output:
pay_dt    string
uid       string
order_id  string
amt       double
channel   string  
```

# Baseline: The Usual Way
Before a purchase, a `uid` may login several times from varying channels. We can left-join `dw_pay` with `dw_boot` by the `uid` column, and then calculate the time-span between each pair of login-pay logs. The gap is formallized as $$\Delta = pay\_dt-boot\_dt\gt0 $$. And finally, for each payment, we just keep the pair with the smallest $$\Delta \gt0 $$. With HQL, it can be implemented as follows:
```sql
-- SQL1
with cte1 as (
    select
        t1.order_id
        ,t1.uid
        ,t1.amt
        ,t1.pay_dt
        ,t2.boot_dt
        ,(t1.pay_dt - t2.boot_dt) as gap
        ,t2.channel
    from dw_pay t1
    left join dw_boot t2 on t1.uid = t2.uid
    where t1.pay_dt > t2.boot_dt
)
,cte2 as (
    select
        order_id
        ,uid
        ,amt
        ,pay_dt
        ,boot_dt
        ,channel
        ,row_number() over(partition by order_id order by gap) as rn
    from cte1
)
select
    order_id
    ,uid
    ,amt
    ,pay_dt
    ,channel
from cte2
where rn = 1
;
```
The execution plan has two major jobs:   
```tex
STAGE DEPENDENCIES:
  Stage-5 is a root stage , consists of Stage-1
  Stage-1
  Stage-2 depends on stages: Stage-1
  Stage-0 depends on stages: Stage-2

STAGE PLANS:
  Stage: Stage-5
    Conditional Operator

  Stage: Stage-1
    Map Reduce
      Map Operator Tree:
          TableScan
            alias: t1
            Statistics: Num rows: 5019 Data size: 1113921 Basic stats: COMPLETE Column stats: NONE
            Reduce Output Operator
              ...
          TableScan
            alias: t2
            Statistics: Num rows: 44675055 Data size: 9470762487 Basic stats: COMPLETE Column stats: NONE
            Reduce Output Operator
              ...
      Reduce Operator Tree:
        Join Operator
          condition map:
               Left Outer Join0 to 1
          keys:
            0 uid (type: string)
            1 uid (type: string)
          outputColumnNames: _col0, _col1, _col2, _col3, _col7, _col9
          Statistics: Num rows: 49142561 Data size: 10417838961 Basic stats: COMPLETE Column stats: NONE
          Filter Operator
            predicate: (_col2 > _col7) (type: boolean)
            Statistics: Num rows: 16380853 Data size: 3472612845 Basic stats: COMPLETE Column stats: NONE
            Select Operator
              ...

  Stage: Stage-2
    Map Reduce
      Map Operator Tree:
          TableScan
            Reduce Output Operator
              ...
      Reduce Operator Tree:
        Select Operator
          expressions: KEY.reducesinkkey0 (type: string), VALUE._col0 (type: string), VALUE._col1 (type: double), VALUE._col2 (type: bigint), KEY.reducesinkkey1 (type: bigint), VALUE._col4 (type: string)
          outputColumnNames: _col0, _col1, _col2, _col3, _col5, _col6
          Statistics: Num rows: 16380853 Data size: 3472612845 Basic stats: COMPLETE Column stats: NONE
          PTF Operator
            Function definitions:
                Input definition
                  input alias: ptf_0
                  output shape: _col0: string, _col1: string, _col2: double, _col3: bigint, _col5: bigint, _col6: string
                  type: WINDOWING
                Windowing table definition
                  input alias: ptf_1
                  name: windowingtablefunction
                  order by: _col5
                  partition by: _col0
                  raw input shape:
                  window functions:
                      window function definition
                        alias: _wcol0
                        name: row_number
                        window function: GenericUDAFRowNumberEvaluator
                        window frame: PRECEDING(MAX)~FOLLOWING(MAX)
                        isPivotResult: true
            Statistics: Num rows: 16380853 Data size: 3472612845 Basic stats: COMPLETE Column stats: NONE
            Filter Operator
              predicate: (_wcol0 = 1) (type: boolean)
              Statistics: Num rows: 8190426 Data size: 1736306316 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                ...
```
And the statistics:
```
Stage-Stage-1: Map: 76  Reduce: 57   Cumulative CPU: 1579.33 sec   HDFS Read: 1771149942 HDFS Write: 6656096 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 13.7 sec   HDFS Read: 6679409 HDFS Write: 369205 SUCCESS
Total MapReduce CPU Time Spent: 26 minutes 33 seconds 30 msec
OK
Time taken: 140.35 seconds, Fetched: 5019 row(s)
```
_SQL1_ joins two fact tables, which may not utilize map-join since they may contain huge amounts of data. The intermedia data set produced by left-join are then fed to a shuffle phase to execute the `row_number` clause. Here, we have to pay for the price of heavy I/O.
# The Unusual Way
Let's assume for now we have only MapReduce at our toolbox, can we solve the problem? The answer is a sound YES. It's easily observed that our problem can be solved with the paradigm of MapReduce. The design is rather simple:

1. Take the two tables as inputs, at the `map` method, assign a tag to identify which table the row is from. Let's name this tag `flag`, and it has two possible values: 1 for rows from `dw_login` and 0 for rows from `dw_pay`.
1. Shuffle by `uid`, and order by `dt`.
1. At the reduce side, maintain a variable that records the current `channel`. When a login record (with 1 as its `flag`) is seen, update the value of `channel`; when a purchase record (with 0 as its `flag`), assign the current value of `channel` to it.
1. Keep the rows whose `flag` are 0 while discarding others.

While the programming paradigm of MapReduce is simple, writing and tuning a MapReduce program is not. Is it possible to hack Hive to attain the same goal?

Custom mappers and reducers is supported by Hive with the syntax `transform`. It works quite similar to Hadoop's streaming functionality. Data are fed to custom scripts from stdin and results are dumped to stdout.  `transform`  makes programming with Hive more flexible as users can code their processing logics in  languages other than Java. But it comes with extra performance penalty since inter-process data communication is introduced. Also, the introduction of yet another language usually means a downgrade of the maintainability of the whole project. In my opinion, this should be our last resort rather than first choice. So in this post, I'm offering a native (though not so intuitive) solution, i.e. a user-defined function.

Hive's UDF API can be classified into three categories by the quantitative relation of their input and output: the 1-to-1 UDF, the many-to-1 UDAF, and the 1-to-many-UDTF. But these well-known types seem useless to our problem. What we need, is a `row_number`-like analytics function. While possessing a 1-to-1 input-output relation, we also need to be able to do partitioning on certain columns and consequently do some calculations under the context of an individual partition.


How exactly does Hive achieve the functionality of `row_number`? It turns out the intuition is quite simple. From the [source code](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFRowNumber.java) of Hive we know that its implementation relies on the interface  `AbstractGenericUDAFResolver`, which happens to be the interface we've been using to develop UDAFs. Compared with UDAFs like `sum` and `max`, even fewer methods are mandatory. Since they don’t need aggregations, analytics functions only work under `GenericUDAFEvaluator.Mode.COMPLETE` mode. So all that’s needed is to implement the `iterate` method.


Can we imitate the implementation of `row_number` to define our own analytics function? Let's try. What we are looking for, is an analytics function (calling it `recent` for now) that can be used with SQL in this way:
```sql
..., recent(flag, channel) over(partition by uid order by dt) as mychannel
```
Here the `partition by` and `order by` are natively supported by Hive, so all we need to do is define the logics within a sorted partition.


It turns out the only methods that need concrete implementation are `iterate` and `terminate`. Inside the configuration method `init`, we enforce the mode to be `COMPLETE` by simply throwing an exception if it's not. The methods `merge` and `terminatapartial` works with modes `PARTIAL1`, `PARTIAL2`, `FINAL`, so exceptions are thrown if they are reached. This is what our implementation looks like:
```java
// for processing an individual record
public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    assert (parameters.length == 2);
    RctAgg myagg = (RctAgg) agg;
    int flg = PrimitiveObjectInspectorUtils.getInt(parameters[0], flgInputOI);
    if (flg == 0) {
        // if record is from anchor table
        myagg.add();
    } else if (flg == 1) {
        // if from look table, update current src and add default value
        myagg.update(parameters[1]);
        myagg.addDefault();
    } else {
        throw new HiveException("unknown flag: " + flg + " must be 0 or 1");
    }

}

// for the output
public Object terminate(AggregationBuffer agg) throws HiveException {
    return ((RctAgg) agg).srcs;
}

static class RctAgg extends AbstractAggregationBuffer {
    //should generate one value for each row in the partition
    ArrayList<Object> srcs;
    Object currSrc;
    ObjectInspector srcOI;

    //...
    void add() {
        srcs.add(ObjectInspectorUtils.copyToStandardObject(currSrc, srcOI,
                ObjectInspectorUtils.ObjectInspectorCopyOption.DEFAULT));
    }

    void addDefault() {srcs.add(null);}

    void update(Object src) {this.currSrc = src;}
}

```
The complete code can be found [here](https://github.com/damientseng/Dive/blob/master/src/main/java/com/damientseng/dive/ql/udf/GenericUDAFRecent.java). An object of type `RctAgg` maintains an `ArrayList` for buffering the intermediate results. This list is later returned to the PTF. The PTF assumes that each value in the buffer corresponds to one record in the partition. So we have to make sure that the size of the buffer is identical to that of the partition, and the order is maintained the same as when they arrive.  

Now let's test the code locally with the help of **junit**. The following test case mocks the lifecycle of a UDAF object. First, the UDAF object is created as the variable `recent`. Then, the handler to `recent` 's `GenericUDAFEvaluator` is obtained as `eval` , which is initialized with mode `GenericUDAFEvaluator.Mode.COMPLETE` . Some data are put through  `eval`'s `iterate` method which does the really work. And finally, the method `terminate` gives the evaluated result. 

```java
public class GenericUDAFRecentTest extends TestCase {

    public void testRecent() throws HiveException {
        GenericUDAFRecent recent = new GenericUDAFRecent();

        GenericUDAFEvaluator eval = recent.getEvaluator(
                new TypeInfo[]{TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo});

        ObjectInspector loi = eval.init(GenericUDAFEvaluator.Mode.COMPLETE,
                new ObjectInspector[]{PrimitiveObjectInspectorFactory.writableIntObjectInspector,
                        PrimitiveObjectInspectorFactory.writableStringObjectInspector});

        GenericUDAFEvaluator.AggregationBuffer buffer = eval.getNewAggregationBuffer();

        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("95")});  // null
        eval.iterate(buffer, new Object[]{new IntWritable(1), new Text("27")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("86")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("24")});  // 27
        eval.iterate(buffer, new Object[]{new IntWritable(1), new Text("08")});  // 08
        eval.iterate(buffer, new Object[]{new IntWritable(0), new Text("76")});  // 08

        Object output = eval.terminate(buffer);

        Object[] expected = {null,
                new Text("27"), new Text("27"), new Text("27"),
                new Text("08"), new Text("08")};
        Assert.assertArrayEquals(expected, ((StandardListObjectInspector) loi).getList(output).toArray());
    }
  // ...
```


After a bit of hacking and testing, now let's see if it works. We are creating two temporary tables, `tmp_boot` for logins and `tmp_order` for payments.
```sql
--SQL2
create table tmp_boot
as select
    stack(4
        ,'i1', '2020-01-01 14:23:16', 'ch1'
        ,'i1', '2020-01-01 15:24:16', 'ch2'
        ,'i1', '2020-01-01 19:24:16', 'ch3'
        ,'i2', '2020-01-01 16:23:15', 'ch4'
) as (uid, dt, ch)  -- uid, login time, login channel
;

create table tmp_order
as select
    stack(6
        ,'i1', '2020-01-01 14:23:15', 13 -- exp: null
        ,'i1', '2020-01-01 15:24:17', 15 -- exp: ch2
        ,'i1', '2020-01-01 16:25:17', 17 -- exp: ch2
        ,'i2', '2020-01-01 14:23:15', 23 -- exp: null
        ,'i2', '2020-01-01 15:24:16', 25 -- exp: null
        ,'i2', '2020-01-01 17:25:17', 27 -- exp: ch4
    ) as (uid, dt, amt)  -- uid, pay time, amount
;
```
Then load the jar and create a UDF named `recent`. And finally, we try to use this function to get the most recent login channel for each payment.
```sql
--SQL3
with cte1 as (
    select
        uid, dt, ch, null as amt,
        1 as flg
    from tmp_boot
    union all
    select
        uid, dt, null as ch, amt,
        0 as flg
    from tmp_order
),
cte2 as (
    select
        uid, dt, amt, flg,
        recent(flg, ch) over(partition by uid order by dt) as mch
    from cte1
)
select
    uid, dt, amt, mch
from cte2
where flg = 0
;
```
The execution plan is much shorter, there's only one job. At the reduce operator tree, we see a window function name `recent` being applied.
```
STAGE DEPENDENCIES:
  Stage-1 is a root stage
  Stage-0 depends on stages: Stage-1

STAGE PLANS:
  Stage: Stage-1
    Map Reduce
      Map Operator Tree:
          TableScan
            alias: tmp_boot
            Statistics: Num rows: 4 Data size: 1104 Basic stats: COMPLETE Column stats: NONE
            Select Operator
              expressions: imei (type: string), dt (type: string), ch (type: string), UDFToInteger(null) (type: int), 1 (type: int)
              outputColumnNames: _col0, _col1, _col2, _col3, _col4
              Statistics: Num rows: 4 Data size: 1104 Basic stats: COMPLETE Column stats: NONE
              Union
                Statistics: Num rows: 10 Data size: 2262 Basic stats: COMPLETE Column stats: NONE
                Reduce Output Operator
                  ...
          TableScan
            alias: tmp_order
            Statistics: Num rows: 6 Data size: 1158 Basic stats: COMPLETE Column stats: NONE
            Select Operator
              expressions: imei (type: string), dt (type: string), '-1' (type: string), amt (type: int), 0 (type: int)
              outputColumnNames: _col0, _col1, _col2, _col3, _col4
              Statistics: Num rows: 6 Data size: 1158 Basic stats: COMPLETE Column stats: NONE
              Union
                Statistics: Num rows: 10 Data size: 2262 Basic stats: COMPLETE Column stats: NONE
                Reduce Output Operator
                  ...
      Reduce Operator Tree:
        Select Operator
          expressions: KEY.reducesinkkey0 (type: string), KEY.reducesinkkey1 (type: string), VALUE._col0 (type: string), VALUE._col1 (type: int), VALUE._col2 (type: int)
          outputColumnNames: _col0, _col1, _col2, _col3, _col4
          Statistics: Num rows: 10 Data size: 2262 Basic stats: COMPLETE Column stats: NONE
          PTF Operator
            Function definitions:
                Input definition
                  input alias: ptf_0
                  output shape: _col0: string, _col1: string, _col2: string, _col3: int, _col4: int
                  type: WINDOWING
                Windowing table definition
                  input alias: ptf_1
                  name: windowingtablefunction
                  order by: _col1
                  partition by: _col0
                  raw input shape:
                  window functions:
                      window function definition
                        alias: _wcol0
                        arguments: _col4, _col2
                        name: recent
                        window function: GenericUDAFRecentEvaluator
                        window frame: PRECEDING(MAX)~FOLLOWING(MAX)
                        isPivotResult: true
            Statistics: Num rows: 10 Data size: 2262 Basic stats: COMPLETE Column stats: NONE
            Filter Operator
              predicate: (_col4 = 0) (type: boolean)
              Statistics: Num rows: 5 Data size: 1131 Basic stats: COMPLETE Column stats: NONE
              Select Operator
                ...
```
The output is exactly what we are expecting.
```sql
i1 2020-01-01 14:23:15 13 NULL
i1 2020-01-01 15:24:17 15 ch2
i1 2020-01-01 16:25:17 17 ch2
i2 2020-01-01 14:23:15 23 NULL
i2 2020-01-01 15:24:16 25 NULL
i2 2020-01-01 17:25:17 27 ch4
```


It's thrilling to see that our odd UDAF works. But wait, does it perform better than pure native code like our _SQL1_? Let's apply `recent` to the real-life case.
```sql
-- SQL4
with cte1 as (
    select
        null as order_id, uid, boot_dt as dt,
        channel, null as pay_amt,
        1 as flg
    from dw_boot
    union all
    select
        order_id, uid, pay_dt as dt,
        null as channel, pay_amt,
        0 as flg
    from dw_pay
),
cte2 as (
    select
        order_id, uid, dt, pay_amt, flg,
        recent(flg, channel) over(partition by uid order by dt) as mch
    from cte1
)
select
    order_id, uid, dt, pay_amt, mch
from cte2
where flg = 0
;
```
```
Stage-Stage-1: Map: 76  Reduce: 57   Cumulative CPU: 1783.87 sec   HDFS Read: 1771359322 HDFS Write: 389191 SUCCESS
Total MapReduce CPU Time Spent: 29 minutes 43 seconds 870 msec
OK
Time taken: 97.07 seconds, Fetched: 5019 row(s)
```
_SQL4_ generates one job, and it is much faster than _SQL1_. Our efforts finally paid off.
## One Step Further
Just like the implementation of `row_numer` in Hive's early versions, our `recent` uses an `ArrayList` to maintain the intermediate results of a whole partition. Simple as it is, this design is not scalable. When the partition size is big, it's not surprising to see OOM exceptions. For our case, if `mapreduce.reduce.memory.mb` is set to _4G_, we occasionally see OOM when a single partition passes  100 million rows. This limit is fairly enough to handle most businesses. But still, we can take our novel (yet fancy) code one step further.

Recall the part where we designed our workflow pretending we can only use MapReduce. When evaluating one row for it's `channel`, only rows before the current one is effective, and we never look any row after it. So obviously we don’t need to cache the whole partition in memory. This is a typical streaming process (see [HIVE-13936](https://issues.apache.org/jira/browse/HIVE-13936)). What we should do is to implement yet another interface `ISupportStreamingModeForWindowing`.
```java
public static class GenericUDAFRecentEvaluator extends GenericUDAFAbstractRecentEvaluator
        implements ISupportStreamingModeForWindowing {

    public Object getNextResult(AggregationBuffer agg) throws HiveException {
        return ((RctAgg) agg).srcs.get(0);
    }

    public GenericUDAFEvaluator getWindowingEvaluator(WindowFrameDef wFrmDef) {
        isStreamingMode = true;
        return this;
    }
    ...
```
The complete code is [here](https://github.com/damientseng/Dive/blob/master/src/main/java/com/damientseng/dive/ql/udf/GenericUDAFRecent.java). Every time after `GenericUDAFEvaluator#iterate` is called for one row, the method `getNextResult` is invoked to get the result of this row. 

The testing of this implementation is a bit more complicated as we need to mock a `WindowFrameDef` object. Here, the definition of  ``WindowFrameDef`` can be interpreted as `rows between unbounded preceding and current row` in HQL.

```java
public void testStreamingRecent() throws HiveException {

    Iterator<Integer> inFlags = Arrays.asList(0, 1, 0, 0, 1, 0).iterator();
    Iterator<String> inSrcs = Arrays.asList("95", "27", "86", "24", "08", "76").iterator();
    Iterator<Text> outVals = Arrays.asList(null,
            new Text("27"), new Text("27"), new Text("27"),
            new Text("08"), new Text("08")).iterator();
  
    int inSz = 6;
    Object[] in = new Object[2];
  
    TypeInfo[] inputTypes = {TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo};
    ObjectInspector[] inputOIs = {PrimitiveObjectInspectorFactory.writableIntObjectInspector,
            PrimitiveObjectInspectorFactory.writableStringObjectInspector};

    GenericUDAFRecent fnR = new GenericUDAFRecent();
    GenericUDAFEvaluator fn = fnR.getEvaluator(inputTypes);
    fn.init(GenericUDAFEvaluator.Mode.COMPLETE, inputOIs);
    fn = fn.getWindowingEvaluator(new WindowFrameDef(
            WindowingSpec.WindowType.ROWS,
            new BoundaryDef(WindowingSpec.Direction.PRECEDING, WindowingSpec.BoundarySpec.UNBOUNDED_AMOUNT),
            new BoundaryDef(WindowingSpec.Direction.CURRENT, 0)));

    GenericUDAFEvaluator.AggregationBuffer agg = fn.getNewAggregationBuffer();

    ISupportStreamingModeForWindowing oS = (ISupportStreamingModeForWindowing) fn;

    int outSz = 0;
    while (inFlags.hasNext()) {
        in[0] = new IntWritable(inFlags.next());
        in[1] = new Text(inSrcs.next());

        fn.aggregate(agg, in);
        Object out = oS.getNextResult(agg);
        assertEquals(out, outVals.next());
        outSz++;
    }

    fn.terminate(agg);
    assertEquals(outSz, inSz);
}
```

Extra tests and real-life use-cases show that the streaming version of `recent` is capable of handling partitions with hundreds of millions of records.

# References
[1] [Hive Windowing and Analytics](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics)  
[2] [GenericUDAFRowNumber](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/GenericUDAFRowNumber.java)  
[3] [HIVE-13936](https://issues.apache.org/jira/browse/HIVE-13936)  
[4] [HIVE-7062](https://issues.apache.org/jira/browse/HIVE-7062)  
[5] [GenericUDAFRecent](https://github.com/damientseng/sak/blob/master/src/main/java/com/damientseng/sak/hive/ql/udf/GenericUDAFRecent.java)  
