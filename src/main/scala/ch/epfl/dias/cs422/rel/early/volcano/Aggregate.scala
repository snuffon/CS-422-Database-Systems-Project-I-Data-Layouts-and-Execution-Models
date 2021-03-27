package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}


class Aggregate protected(input: Operator,
                          groupSet: ImmutableBitSet,
                          aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  private var groupMap = Map[ListBuffer[Any], ListBuffer[Tuple]]()
  private var groups = ArrayBuffer[ListBuffer[Any]]()
  private var groupIndex = 0
  private var emptyCase = false

  override def open(): Unit = {
    //println("aggregate open")
    input.open()
    var tuple = input.next()
    if (tuple == null)
      emptyCase = true
    while (tuple != null) {
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
      tuple = input.next()
    }
  }

  override def next(): Tuple = {
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
      return aggs.toIndexedSeq
    } else {
      //println((group ++ aggs).toIndexedSeq)
      (group ++ aggs).toIndexedSeq
    }
  }

  override def close(): Unit = {
    input.close()
  }
}
