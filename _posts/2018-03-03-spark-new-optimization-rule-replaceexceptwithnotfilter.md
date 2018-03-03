---
ID: 275
post_title: 'Spark New Optimization Rule &#8211; ReplaceExceptWithNotFilter'
author: sathyak_1989
post_excerpt: ""
layout: post
permalink: >
  http://sathiyaprabhu.com/spark-new-optimization-rule-replaceexceptwithnotfilter/
published: true
post_date: 2018-03-03 17:29:05
---
# Spark New Optimization Rule - ReplaceExceptWithNotFilter

Spark community decided to replace Except logical operator using left anti-join in [SPARK-12660][1]. It facilitates to take advantage of all the benefits of the join operations such as managed memory, code generation and broadcast joins, cc. [SPARK-12660][1].

Except is one of the mostly used logical operator that is mainly used to get the difference between the two datasets. More often (not always), we happen to use except on two different datasets that are derived/ transformed from a single parent dataset. If two datasets parent relations (say., HDFS File) are same and one or both of the datasets (on which we need to run apply except) are purely transformed using filter operations, then instead of rewriting the Except operator using expensive join operation, we can rewrite it using cheaper filter operation. I will discuss the details of this rule in the blog post. If you are not already familiar with the internals of spark catalyst and it's Tree Node structure and query optimization refer my previous post [here]() before continuing with this post.

## Quick Example

Case 1: 

![replace_except_with_not_filter_case-1][2]



Case 2: 

![replace_except_with_not_filter_case-2][3]



## Logical Operators

Here is a brief introduction about the three logical operators that is concerned with this post.

### Except operator

Except operation takes two datasets d1 and d2 and returns a resulting dataset with the rows that don't appear in the second dataset. In order to compare the tuples from the two datasets both the datsets should contains same number of columns/ fields or at least has to be projected in that way. There are two types of except operation based on whether to remove duplicates or not.

*   Except All: Don't remove duplicates

*   Except Distinct: Removes duplicates

In spark there is no such distinction between Except All and Except Distinct, there is only one operator Except, which actually Except Distinct.

### Anti Join operator

Anti join is a better way of doing queries that is traditionally used to be done using `NOT IN` or `NOT EXISTS` operator. The performance benefits of using anti join over `NOT IN` or `NOT Exists` operator is similar to performance of Nested loop join over Hash join. You can read more about it at this [link][4].

Eg:

<pre><code class="sql">select * from table1 t1 where not exists (select 1 from table2 t2 where t1.id = t2.id)
</code></pre>

Anti join don't remove the duplicates, so the results of a query using anti join will be same as that of the results of the `Except All` operator.

### Distinct operator

Distinct operation removes duplicate tuples in a dataset. In order to do this spark does Hash partitioning on the dataset using all the columns of the dataset as the partitioning key and uses the hash code of the tuples for removing the duplicates. Distinct operation is actually expensive than doing an anti join operation.

## Spark Optimizer

Enough of the theory, let's take a look at some code from the spark codebase.

Below is a snippet from spark Logical query optimizer: `org.apache.spark.sql.catalyst.optimizer.Optimizer`

<pre><code class="scala">::
    Batch("Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) 
::      
</code></pre>

And here is the actual `ReplaceExceptWithAntiJoin` Rule implementation.

<pre><code class="scala">object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right) =&gt;
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) =&gt; EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And)))
  }
}
</code></pre>

By seeing this rule, it's tempted me to write a new optimization rule called `ReplaceExceptWithNotFilter` and schedule it just before the `ReplaceExceptWithAntiJoin` rule and check what happens. Here is the code, it's not necessarily to be perfect as i'm still experimenting with it.

<pre><code class="scala">::
    Batch("Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,
      ReplaceExceptWithNotFilter,
      ReplaceExceptWithAntiJoin,
      ReplaceDistinctWithAggregate) 
::      
</code></pre>

<pre><code class="scala">object ReplaceExceptWithNotFilter extends Rule[LogicalPlan] {

  implicit def nodeToFilter(node: LogicalPlan): Filter = node.asInstanceOf[Filter]

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right) if isEligible(left, right) =&gt;
      Distinct(
        Filter(Not(replaceAttributesIn(combineFilters(right).condition, left)), left)
      )
  }

  def isEligible(left: LogicalPlan, right: LogicalPlan): Boolean = (left, right) match {
    case (left: Filter, right: Filter) =&gt; parent(left).sameResult(parent(right))
    case (left, right: Filter) =&gt; left.sameResult(parent(right))
    case _ =&gt; false
  }

  def parent(plan: LogicalPlan): LogicalPlan = plan match {
    case x @ Filter(_, child) =&gt; parent(child)
    case x =&gt; x
  }

  def combineFilters(plan: LogicalPlan): LogicalPlan = CombineFilters(plan) match {
    case result if !result.fastEquals(plan) =&gt; combineFilters(result)
    case result =&gt; result
  }

  def replaceAttributesIn(condition: Expression, leftChild: LogicalPlan): Expression = {
    condition transform {
      case AttributeReference(name, _, _, _) =&gt;
        leftChild.output.find(_.name == name).get
    }
  }
}
</code></pre>

I tested the code on a csv file of size 500 Mb using couple of quick queries something as follows:

<pre><code class="sql"> val ds1 = spark.read.option("header", "true").csv("path/to/the/dataset")
 val ds2 = ds1.where($"month" === 12)
 val ds3 = ds1.where($"month" &lt; 3)
 val ds4 = ds3.except(ds2)
</code></pre>

And when i verified the optimized plan via `ds4.queryExecution.optimizedPlan.numberedTreeStringd`, the plan is changed

from:

<pre><code class="sql">00 Aggregate [...]
01 +- Join LeftAnti, (...)
02    +- Relation[...]
</code></pre>

to:

<pre><code class="sql">00 Aggregate [...]
01 +- Filter ((isnotnull(month) && (cast(month as int) &lt; 3)) && NOT (cast(month as int) = 12))
02    +- Relation[...]
</code></pre>

Apparently the new rule `ReplaceExceptWithNotFilter` gave a good performance gain. The query that took around 60 seconds is reduced to 26 seconds with the addition of the new rule. My immediate thought was when there is 120% latency gain on a standalone instance where there is no actual shuffling involved, there should be a much better performance gain in a big cluster. So i wanted to add this new rule to the spark Optimizer via `ExperimentalMethods` that is available since spark 2.0 (thanks to [SPARK-9843][5]) and verify it's performance in a production cluster.

When i did so, the new rule is never applied by the spark optimizer. When i did some debugging, i found the extra optimizations added via `ExperimentalMethods` are applied only after all the batches of predefined are rules applied. By the time the new `ReplaceExceptWithNotFilter` rule scheduled to apply, the `Except` operator is already replaced with the `Anti-Join` operation by the predefined rules. It would have been helpful if the `ExperimentalMethods` class provides option to add some pre-optimization rules here. I will discuss how to make this option available in the [next post][6].

So in order to test the performance of this new rule i wrote an another rule `ReplaceAntiJoinWithNotFilter` that rewrites the anti join operation with a filter operator. Here is that custom rule:

<pre><code class="scala">object ReplaceAntiJoinWithNotFilter extends Rule[LogicalPlan] {

  implicit def nodeToFilter(node: LogicalPlan) = node.asInstanceOf[Filter]

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Join(left, right, joinType, _) if joinType.sql == "LEFT ANTI" && isEligible(left, right) =&gt;
      Filter(Not(replaceAttributesIn(right.condition, left)), left)
  }

  def isEligible(left: LogicalPlan, right: LogicalPlan): Boolean = (left, right) match {
    case (_ @ Filter(_, lChild: LogicalRelation), _ @ Filter(_, rChild: LogicalRelation)) =&gt;
      equals(lChild, rChild)
    case (leftNode: LogicalRelation, _ @ Filter(_, rChild: LogicalRelation)) =&gt;
      equals(leftNode, rChild)
    case _ =&gt; false
  }

  def equals(leftNode: LogicalRelation, rightNode: LogicalRelation): Boolean = {
    leftNode.relation == rightNode.relation
  }

  def replaceAttributesIn(condition: Expression, leftChild: LogicalPlan): Expression = {
    condition transform {
      case AttributeReference(name, _, _, _) =&gt;
        leftChild.output.find(_.name == name).get
    }
  }
}
</code></pre>

Finally, i managed to test the `ReplaceAntiJoinWithNotFilter` rule on a production Hadoop cluster with the resources `--executor-memory 110G --total-executor-cores 250` on a csv file of 10 Gb. A similar query as we seen above that takes 3 minutes is reduced to 20 seconds, which is actually an order of magnitude difference.

One of the limitations of applying the custom rule at the end is that the custom rule won't be subjected to any further optimization. For example, when i scheduled to apply the `ReplaceExceptWithNotFilter` rule just before the `ReplaceExceptWithAntiJoin` rule in the "Replace Operators" batch, the filter operation is further optimized by some of the spark predefined optimization rules

from:

<pre><code class="sql">01 +- Filter NOT (isnotnull(month) && (cast(month as int) = 12))
02    +- Filter (isnotnull(month) && (cast(month as int) &lt; 3))
</code></pre>

to:

<pre><code class="sql">01 +- Filter ((isnotnull(month) && (cast(month as int) &lt; 3)) && NOT (cast(month as int) = 12))
</code></pre>

But in case of adding the `ReplaceAntiJoinWithNotFilter` rule via `ExperimentalMethods`, the same optimization is not effected. So while applying any custom rules at the end via `ExperimentalMethods`, we should make sure it is fully optimized, unless `ExperimentalMethods` class provides an option to apply our custom rules before spark predefined rules are applied. Let's see how to open this option in `ExperimentalMethods` in less than 10 lines of codes in my [next post][6].

 [1]: https://issues.apache.org/jira/browse/SPARK-12660
 [2]: images/spark-optimizer/ReplaceExceptWithNotFilter-case1.png
 [3]: images/spark-optimizer/ReplaceExceptWithNotFilter-case2.png
 [4]: https://technet.microsoft.com/en-us/library/ms191318(v=sql.105).aspx
 [5]: https://issues.apache.org/jira/browse/SPARK-9843
 [6]: https://github.com/sathiyapk/Blog-Posts/blob/master/SparkOptimizer.md