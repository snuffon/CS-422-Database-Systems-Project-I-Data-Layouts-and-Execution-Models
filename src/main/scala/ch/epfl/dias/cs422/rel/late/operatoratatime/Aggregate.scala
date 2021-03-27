package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  private var groupMap = Map[ListBuffer[Any], ListBuffer[Tuple]]()
  private var groups = ArrayBuffer[ListBuffer[Any]]()
  private var groupIndex = 0
  private var emptyCase = false
  var columns = getColumns(input)

  if (columns == null || columns.length == 0 || columns(0).length == 0)
    emptyCase = true
  else {
    for (r <- 0 until columns(0).length) {
      var t = ListBuffer[Any]()
      for (c <- 0 until columns.length) {
        t += columns(c)(r)
      }
      val tuple = t.toIndexedSeq

      var group = ListBuffer[Any]()
      for (i <- 0 until groupSet.length()) {
        if (groupSet.get(i))
          group += tuple(i)
      }
      if (!groupMap.contains(group)) {
        groups += group
        groupMap += (group -> ListBuffer[Tuple]())
      }
      groupMap(group) = groupMap(group) :+ tuple
    }
    columns = null
  }

  // Exact same as volcano
  private def next(): Tuple = {
    if (groupIndex >= groups.length) {
      if (emptyCase) {
        emptyCase = false
        var aggs = ListBuffer[Any]()
        for (aggCall <- aggCalls) {
          aggs += aggCall.emptyValue
        }
        return aggs.toIndexedSeq
      }
      //println("aggregate done")
      return null
    }
    val group = groups(groupIndex)
    groupIndex += 1
    val tuples = groupMap(group)
    var aggs = ListBuffer[Any]()
    for (aggCall <- aggCalls) {
      var agg = aggCall.getArgument(tuples.head)
      for (tuple <- tuples.tail) {
        agg = aggCall.reduce(agg, aggCall.getArgument(tuple))
      }
      aggs += agg
    }
    if (groups(0).length == 0) {
      aggs.toIndexedSeq
    } else {
      //println((group ++ aggs).toIndexedSeq)
      (group ++ aggs).toIndexedSeq
    }
  }

  private def executeMaterialized(): IndexedSeq[Column] = {
    var tuples = ArrayBuffer[Tuple]()
    var tuple = next()
    if (tuple == null)
      return IndexedSeq[IndexedSeq[Any]]()
    while (tuple != null) {
      tuples += tuple
      tuple = next()
    }

    val result = Array.fill(tuples(0).length)(ListBuffer[Any]())
    for (t <- tuples) {
      for (c <- 0 until t.length) {
        result(c) += t(c)
      }
    }
    var cs = ListBuffer[IndexedSeq[Any]]()
    for (c <- 0 until result.length) {
      cs += result(c).toIndexedSeq
    }
    cs.toIndexedSeq
  }


  private def getColumns(op: Operator): IndexedSeq[Column] = {
    val ids = op.execute().flatten
    if (ids.length == 0)
      return IndexedSeq[IndexedSeq[Any]]()
    val evals = op.evaluators()
    var cols = ArrayBuffer[ListBuffer[Any]]()
    var tupl = evals.apply(IndexedSeq(ids(0)))
    for (c <- 0 until tupl.length)
      cols += ListBuffer[Any]()
    for (id <- ids) {
      tupl = evals.apply(IndexedSeq(id))
      for (c <- 0 until tupl.length)
        cols(c) += tupl(c)
    }
    var result = ListBuffer[IndexedSeq[Any]]()
    for (c <- 0 until tupl.length)
      result += cols(c).toIndexedSeq
    result.toIndexedSeq
  }

  private var l = ListBuffer[Long => Any]()
  private var x = IndexedSeq[IndexedSeq[Any]]()
  val cs0 = executeMaterialized()
  if (cs0.length != 0) {

    for (c <- cs0) {
      val f: (Long => Any) = long => c(long.toInt)
      l += f
    }
    var x1 = ListBuffer[IndexedSeq[Long]]()
    for (i <- 0 until cs0(0).length)
      x1 += List(i.toLong).toIndexedSeq
    x = x1.toIndexedSeq
  }


  override def execute(): IndexedSeq[Column] = {
    x
  }

  private lazy val evals = new LazyEvaluatorAccess(l.toList)

  override def evaluators(): LazyEvaluatorAccess = evals
}
