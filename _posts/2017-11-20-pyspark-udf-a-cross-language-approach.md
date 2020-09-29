---
layout: post
title: "PySpark UDF, a Cross-Language Approach"
author: "Damien Tseng"
categories: Spark
tags: [Spark]
mathjax: true
---
Apache Spark supports SQL interface for data processing. The functionalities provided by SparkSQL are quite general and thorough. Yet user-defined aggregate function is a must for working with SQL environments. This post discusses how to utilize efficient UDFs and UDAFs in PySpark.  

# The Problem
Although Spark supports implementing UDFs in Python, the performance is much slower than UDFs written in Scala or Java. Due to the design of PySpark, we need a way to push the real workload to the JVM side to alleviate this performance gap. Even worse, UDAFs are limited to JVM languages only, so it's not possible to realize UDAFs in Python for now. In order to address these problems, we need a little bit of knowledge about how PySpark works.
# How PySpark Works
PySpark is designed on top of Spark Core, which as you might have known is mostly implemented in Scala. PySpark has a multi-process architecture. There are two kinds of processes on the driver side as well as the executor side: JVM processes and Python processes. On the driver side, a JVM process is started by the Python process with the help of Py4J. While on the executor side, Python processes are launched by JVMs.
![pyspark-architecture.png](/assets/images/pyspark-architecture.png)
On the executor side, where heavy data processing works are performed, data reside in JVM. In order to execute data processing logics defined in Python UDFs, data are transferred to Python processes for processing and then transferred back through pipes. Here, inter-process communication requires data serialization and deserialization. This is the root cause of Python UDFs' performance loss.


When creating the Python `SparkContext` object on the driver side, a JVM is launched and a corresponding Java `SparkContext` object is initialized.
```python
"""
https://github.com/apache/spark/blob/4d2d3d47e00e78893b1ecd5a9a9070adc5243ac9/python/pyspark/context.py#L115
"""
SparkContext._ensure_initialized(self, gateway=gateway, conf=conf)
try:
    self._do_init(master, appName, sparkHome, pyFiles, environment, batchSize, serializer,
                  conf, jsc, profiler_cls)
except:
    # If an error occurs, clean up in order to allow future SparkContext creation:
    self.stop()
    raise
...

"""
https://github.com/apache/spark/blob/4d2d3d47e00e78893b1ecd5a9a9070adc5243ac9/python/pyspark/context.py#L124
"""
# Create the Java SparkContext through Py4J
self._jsc = jsc or self._initialize_context(self._conf._jconf)
# Reset the SparkConf to the one actually used by the SparkContext in JVM.
self._conf = SparkConf(_jconf=self._jsc.sc().conf())
```
Note that `pyspark.context.SparkContext` has a member `_jsc`, which is a wrapper arround the Java `SparkContext` object. This actually is a common pattern for PySpark: there are many Python objects that are wrappers arround JVM objects, which conform to the naming convention like `_jxx`. Here are some more examples: `DataFrame` has a `_jdf`, `RDD` has a `_jrdd`, and most importantly, `SparkSession` has a `_jvm`.


PySpark's DataFrame API DSL calls are interpreted as their `_jxx` counterparts behind the scene.
```python
"""
https://github.com/apache/spark/blob/4d2d3d47e00e78893b1ecd5a9a9070adc5243ac9/python/pyspark/sql/dataframe.py#L373
"""
@since(1.3)
def count(self):
    """Returns the number of rows in this :class:`DataFrame`.

    >>> df.count()
    2
    """
    return int(self._jdf.count())
```


The filed `SparkSession._jvm` is connected to the JVM process through a gateway (a Py4J JavaGateway).
```python
"""
https://github.com/apache/spark/blob/4d2d3d47e00e78893b1ecd5a9a9070adc5243ac9/python/pyspark/context.py#L252
"""
@classmethod
def _ensure_initialized(cls, instance=None, gateway=None, conf=None):
    with SparkContext._lock:
        if not SparkContext._gateway:
            SparkContext._gateway = gateway or launch_gateway(conf)
            SparkContext._jvm = SparkContext._gateway.jvm
        ...
"""
https://github.com/apache/spark/blob/4d2d3d47e00e78893b1ecd5a9a9070adc5243ac9/python/pyspark/java_gateway.py#L118
"""
gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port, auth_token=gateway_secret,
                                             auto_convert=True))
```
From this entry-point, we can access objects living in the JVM. In order to get a better understanding of the functionality, let’s see a hands-on example. Here is a function in Scala:
```scala
package com.damientseng.spark.udf

object operator {
  def increment(a: Int): Int = a + 1
}
```
 After packing to generating a jar file, pyspark is started and the jar file is provided through parameters:
```bash
./bin/pyspark --jars .../sparkudf_2.11-0.1.jar
```
Now, the object `Greeting` is accessible through a full-qualifier reference:
```
>>> spark._jvm.com.damientseng.spark.udf.operator
<py4j.java_gateway.JavaClass object at 0x7fcc7bafb790>
```
As shown above, `spark._jvm.com.damientseng.spark.udf.operator` is a Py4J wrapper around a Java object. And we can actually call the function:
```
>>> spark._jvm.com.damientseng.spark.udf.operator.increment(7)
8
```
Now that we know mysterious mechanism of PySpark, how does this help with our implementation of UDFs ?
# UDF
To boost UDF calls from PySpark, we need a magic to push processing logics to the JVM where data reside. This way, we get to alleviate data serialization/deserialization and communication between Python and JVM processes. We can achieve this in a way similar to calling `operator.increment`. Just one more thing is needed: a way to register the UDF before it is invoked.

One way to achieve this to register the function from the JVM side. We need to define a Scala method that handles the registration, which accepts a `SparkSession` as its parameter. As shown below, the method `register` takes a `SparkSession` and registers our UDF `increment` as `incr` .
```scala
package com.damientseng.spark.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object operator {
  val increment: Function1[Int, Int] = _ + 1

  def register(spark: SparkSession): Unit = {
    spark.udf.register("incr", udf(increment))
  }
}
```
On the PySpark dirver side, a `SparkSession` is wrapped by `spark._jsparkSession`. Let's pass it to `operator.register`:
```
>>> spark._jvm.com.damientseng.spark.udf.operator.register(spark._jsparkSession)
```
Now, the UDF `incr` should be available for invocation.
```
>>> nums = spark.range(3, 7)
>>> nums.show()
+---+
| id|
+---+
|  3|
|  4|
|  5|
|  6|
+---+

>>> nums.createOrReplaceTempView("nums")
>>> spark.sql("select incr(id) as nid from nums").show()
+---+
|nid|
+---+
|  4|
|  5|
|  6|
|  7|
+---+

```


Yet another approach of registering is to do it from the Python side. While on the Scala side, a function is merely created and returned.
```scala
package com.damientseng.spark.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object operator {
  def incr: UserDefinedFunction = udf( (a: Int) => a + 1 )
}

```
The actual registration is somewhat similar to what we did previously. But this time, the function `register` is involved by Python.
```
>>> incr = spark._jvm.com.damientseng.spark.udf.operator.incr()
>>> spark._jsparkSession.udf().register('incr', incr)
JavaObject id=o104
>>> nums = spark.range(1, 5)
>>> nums.createOrReplaceTempView("nums")
>>> spark.sql("select incr(id) as nid from nums").show()
+---+
|nid|
+---+
|  2|
|  3|
|  4|
|  5|
+---+
```


You may find such kind of work-arounds mysterious and awkward. It's true. For one thing, fileds like `_jvm` are prefixed with a single underscore, which indicate they are private variables. Although it's just a convention in Python, these variables are not designed as a part of PySpark's API. So they are not guaranteed to be stable among different versions. Fortunately, starting from version 2.1.0, an official function `registerJavaFunction` is offered for registering UDFs implemented by Java/Scala.
```python
"""
https://github.com/apache/spark/blob/4d2d3d47e00e78893b1ecd5a9a9070adc5243ac9/python/pyspark/sql/context.py#L206
"""
@ignore_unicode_prefix
@since(2.1)
def registerJavaFunction(self, name, javaClassName, returnType=None):
    """Register a java UDF so it can be used in SQL statements.
    ...
    """
    jdt = None
    if returnType is not None:
        jdt = self.sparkSession._jsparkSession.parseDataType(returnType.json())
    self.sparkSession._jsparkSession.udf().registerJava(name, javaClassName, jdt)
```
# UDAF
Spark doesn’t support UDAFs from Python language. Although it's possible to simulate the behavior of a UDAF by combining the built-in UDAF `collect_list` and a plain UDF, the performance is usually low for data sets of real-life scale. A much better alternative is to adopt the strategy described above: implementation by Scala, invocation by Python. Let's walk through an example.


Here is a UDAF written in Scala, which gets the count of odd numbers:
```scala
package com.damientseng.spark.udf

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CountOdd extends UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", IntegerType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("count", LongType) :: Nil)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0L

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + (input.getAs[Int](0) & 1)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}
```
Just as before, the UDAF can be registerd from the Scala side, let's place the `register` function in the companion object of the class `CountOdd`.
```scala
object CountOdd {
  def register(spark: SparkSession): Unit = {
    spark.udf.register("count_odd", new CountOdd)
  }
}
```
After starting the PySpark promt by providing the jar file, we can register and invoke our UDAF from the Python driver side:
```
>>> spark._jvm.com.damientseng.spark.udf.CountOdd.register(spark._jsparkSession)
>>> nums = spark.range(1, 13)
>>> nums.show()
+---+
| id|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
| 10|
| 11|
| 12|
+---+

>>> nums.createOrReplaceTempView("nums")
>>> spark.sql("select count_odd(id) from nums").show()
+-------------------------+
|countodd(CAST(id AS INT))|
+-------------------------+
|                        6|
+-------------------------+

```
And of course, the Python side registration is applicable:
```
>>> count_odd = spark._jvm.com.damientseng.spark.udf.CountOdd()
>>> spark._jsparkSession.udf().register("count_odd", count_odd)
JavaObject id=o114
>>> nums = spark.range(1, 13)
>>> nums.createOrReplaceTempView("nums")
>>> spark.sql("select count_odd(id) from nums").show()
+-------------------------+
|countodd(CAST(id AS INT))|
+-------------------------+
|                        6|
+-------------------------+
```
