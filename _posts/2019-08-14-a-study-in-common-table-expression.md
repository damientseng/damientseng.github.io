---
layout: post
title: "A Study in Common Table Expression"
author: "Damien Tseng"
categories: Big-Data
tags: [MySQL,Hive]
mathjax: true
---
<br />
In the SQL context, the accessibility of subqueries (derived tables) is restricted. When we need to refer to a derived table several times within a single query, one way is to repeat this subquery as many times as it's accessed. This works like a charm until one day you suddenly realize it's kind of ridiculous to write and maintain the same code over and over.

<!-- more -->

After all, such repetition doesn't seem programmatic. On the other hand, there is no easy way for the DBMS to tell whether those derived tables will generate identical result sets. This means that evaluations of these equivalent subqueries are also repeated, leading to compromised execution efficiency. 

This is where Common Table Expressions come to our rescue. Common table expression, CTE for short, is yet another way to represent an intermediate result set in SQL queries. You may also know it as the `with clause`. Introduced by the SQL3 standard, CTE resolved the limitation of derived tables by allowing the resultant data set to be accessed throughout the whole SQL query. So for a query that's written like this without using CTE:
```sql
select
  ta.dt, ta.name, ta.gp, tb.gp
from (
  select dt, name, sum(pay) as gp from trade group by dt, uid
) ta
join (
  select dt, name, sum(pay) as gp from trade group by dt, uid
) tb
on ta.name = tb.name and ta.dt = date_sub(tb.dt, interval 1 day);
```

<br/>Now with CTE, we can rewrite the code as:
```sql
with cte as (
  select dt, name, sum(pay) as gp from trade group by dt, uid
)
select
  ta.dt, ta.name, ta.gp, tb.gp
from cte ta
join cte tb
on ta.name = tb.name and ta.day = date_sub(tb.day, interval 1 day);
```

<br/>The pattern of how we use a CTE is declare-and-refer, much like the way we use a variable or a named function/method in a general-purpose programming language like Java. This pattern comes with better expressiveness and accessibility, which makes code more readable and maintainable.   

It was not until 2017 when version 8.0.1 was released, that MySQL started to support CTE. Now let's see some more examples of CTE, **with** MySQL **as** the environment.

CTEs can be chained, where latter CTEs may rely on former ones. 
```sql
with cte1 as (
  select c from some_table
),
cte2 as (
  select c+2 as cc from cte1
)
select cc from cte2;
```

<br/>CTEs are not exclusively made for `select` clauses, they can also be used with `insert`, `create`, `update`, and even `delete` clauses.
```sql
insert into another_table
with cte as (select c from some_table)
select c from cte;

create table another_table as
with cte as (select c from some_table)
select c from cte;

with seniors as (
  select employee_id from employees where years > 5
)
update seniors s, compensation c
set c.salary = c.salary*1.2
where s.employee_id = c.employee_id;

with cte as (
  select c from some_table where c > 0
)
delete from another_table
where d > (select max(c) from cte);

--Column names can be specifically given,
--whereas their data types are determined by the query.
with cte(gift) as (
  select name from product where type = 'gift'
)
select gift from cte;
```

There's more. CTEs can be made recursive to compose [hierarchical queries](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL).

# Hierarchical Queries
Have you ever wondered, is it possible calculate Fibonacci sequence with pure SQL? The answer is *YES*. Now let's recall the general term formula of Fibonacci sequence:

$$
f(n) = \begin{cases}
  1, & \text{if n=1,2}  \\
  f(n-2) + f(n-1), & \text{else}
\end{cases}
$$

To get the *n-th* number, where $n>2$, our formula refers to itself twice with varying parameters, thus making a typical recursive structure. If your SQL environment is Oracle, it's possible to achieve this sequence using `connect by`. An alternative is to use a recursive CTE, which is supported by Microsoft SQL Server, PostgreSQL, and MySQL 8.0.1+. In MySQL, we may write sth like this:
```sql
with recursive fibo(a, b) as (
    select 1, 1  --initial result set
    union all
    select b, a+b from fibo where b < 100  --how to generate new result sets,
                                           --and when to end.
)
select a from fibo;
+------+
| a    |
+------+
|    1 |
|    1 |
|    2 |
|    3 |
|    5 |
|    8 |
|   13 |
|   21 |
|   34 |
|   55 |
|   89 |
+------+
11 rows in set (0.00 sec)
```

<br/>There is something worth noting about the above query. First, a keyword `recursive` is inserted to explicitly identify a recursive CTE clause. Second, there are two parts within the CTE clause combined by a `union all`. This CTE is recursive in that its definition refers to itself. The `select b, a+b...` part is the recursion body. Although recursive CTE has a recursive definition, its implementation is iterative. You may already know that a recursive implementation of the Fibonacci sequence (without memorization) has a complexity of exponential time, which gets stuck even when *n* is not that large. While with our MySQL example, it's lightning-fast to get the 1000th number. 

Now let's see more examples.
```sql
-- the power of 2
with recursive cte(n) as (
    select 1
    union all
    select n*2 from cte where n < 1024
)
select n from cte;
+------+
| n    |
+------+
|    1 |
|    2 |
|    4 |
|    8 |
|   16 |
|   32 |
|   64 |
|  128 |
|  256 |
|  512 |
| 1024 |
+------+
11 rows in set (0.00 sec)


-- factorial
with recursive fac(acc, n) as (
  select 1, 1
  union all
  select acc*n, n+1 from fac where n <= 10
)
select acc from fac;
+---------+
| acc     |
+---------+
|       1 |
|       1 |
|       2 |
|       6 |
|      24 |
|     120 |
|     720 |
|    5040 |
|   40320 |
|  362880 |
| 3628800 |
+---------+
11 rows in set (0.00 sec)
```

One more thing to note about recursive CTE is its constraints. At the *n-th* iteration, only the partial results generated by the *n-1-th* are accessible, while earlier results are not visible. This limitation leads to unsupported combinations of recursive CTE with `group by`, `order`, `distinct` and so forth, as they rely on being able to see the whole result set. Moreover, recursive CTE can not be the right table (Null Supplying Table) at a `left join`, since we need access to the whole data set to check the existence of a particular key. 
  
# Yet Another Case to Study
The supervisor-subordinate relation in companies and organizations is a typical hierarchical data model. Given the following table, how do we find all the subordinates of *Mark*?
```sql
mysql> select * from employee;
+-------+------------+
| name  | supervisor |
+-------+------------+
| Bryan | Mark       |
| Chris | Mark       |
| Jack  | Mark       |
| Mark  | NULL       |
| Robin | Jack       |
| Will  | Jack       |
+-------+------------+
6 rows in set (0.00 sec)
```

Logically, we can find all direct subordinates of Mark(records whose supervisor columns are *Mark*), call the result set *S1*. Then, we find all employees whose supervisors are contained by *S1*, and we call the result set *S2*... Repeat this pattern until an empty result set is generated. But how do we translate this procedure into code?

## The Non-CTE Way
One straightforward way to go is self-join. The limitation is that the depth of our hierarchy should be defined beforehand. And the query gets complicated if the hierarchy is deep.
```sql
select t1.name, t2.name, t3.name from
  (select name from employee where supervisor = 'Mark') t1
  left join employee t2 on t1.name = t2.supervisor
  left join employee t3 on t2.name = t3.supervisor
  ...
```
## Nested Set Model
A more sophisticated way is to modify the data structure to form a [Nested Set Model](https://en.wikipedia.org/wiki/Nested_set_model). Logically we are trying to label nodes in the hierarchy tree with path boundaries. A tree node has four attributes: a `name` for identification, a `supervisor` pointer referring to its parent node, a `lft` integer representing the lower bound of its subordinates, and a `rgt` integer representing the higher bound of its subordinates. As for the aforementioned table `employee`, the relation tree is as follows. 

![nested-set-model.png](/assets/images/nested-set-model.png)

Take *Jack* for instance, we have `lft=2`, `rgt=7` and any node *X* is one of Jack's subordinates if `Jack.lft<X.lft<X.rgt<Jack.rgt`. This tree can be modeled by a table with four columns like `| name | supervisor | lft | rgt |`. And once this table is generated, it's quite convenient to fulfill our request.

```sql
select
  sub.name
from (
  select * from tree where supervisor = 'Mark'
) super
inner join tree sub
on sub.lft >= super.lft and sub.rgt <= super.rgt
```

<br/>So how do we generate this table? Well, first we need to construct an adjacency list representation of the logical graph. Then, by doing a depth-first search, we can assign `lft` when entering a node and assign `rgt` when exiting one. Here is how a Python implementation would look like.
```python
#-*- table2tree.py -*-

def table2graph(table):
    graph = {sub: [] for sub, _ in table}
    for sub, parent in table:
        if not parent:
            continue
        graph[parent].append(sub)
    return graph

def traverse(graph, root):
    tree = []
    def dfs(parent, node, idx):
        this_left = idx + 1
        curr_idx = this_left
        for child in graph[node]:
            curr_idx = dfs(node, child, curr_idx)
        this_right = curr_idx + 1
        tree.append((node, parent, this_left, this_right))
        return this_right
    dfs(None, root, 0)
    return tree

if __name__ == '__main__':
    t = [('Mark', None),
      ('Jack', 'Mark'),
      ('Chris', 'Mark'),
      ('Bryan', 'Mark'),
      ('Will', 'Jack'),
      ('Robin', 'Jack')]
    from pprint import pprint
    g = table2graph(t)
    pprint(traverse(g, 'Mark'))
```
```python
# output
[('Will', 'Jack', 3, 4),
 ('Robin', 'Jack', 5, 6),
 ('Jack', 'Mark', 2, 7),
 ('Chris', 'Mark', 8, 9),
 ('Bryan', 'Mark', 10, 11),
 ('Mark', None, 1, 12)]
```

## The Recursive CTE Way
After seeing those hideous (kinda) experiments above, we come to realize there's gotta be a better way. How about recursive CTE? This is a perfect use case where recursive CTEs shine! The code is as simple as the following:
```sql
with recursive cte(name) as (
  select
    cast(name as char(50)) as name
  from employee where supervisor = 'Mark'

  union all
  select e.name from employee e join cte on e.supervisor = cte.name
)
select name from cte;
```

# CTE in Hive
CTE is supported by Hive ever since version 0.13.0. We can use CTE with `select`, `insert select`, `create table as select`, `create view as select`, and so on. Then what about recursive CTE? Well, Hive does not support recursive CTE, as the underlying execution engine like MapReduce is not cut out for repetitive/iterative processing. There is no explicit keyword `recursive`, and we can not refer to itself when defining a CTE. 

For Hive, CTE is merely a query-level temp table. CTE works like a syntax sugar that makes user code easier to read by reducing repetition. While under the hood, CTEs are relocated wherever they are referred, and reinterpreted as subqueries. This replacement happens when generating Resolved Parse tree from the syntax tree, way before query logics access actual data. The takeaway here is that a CTE referred more than once may be evaluated more than once and this overhead should be taken into account when trying to optimize your queries. 
