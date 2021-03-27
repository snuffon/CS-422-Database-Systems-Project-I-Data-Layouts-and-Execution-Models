package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  var blockStored = Array.fill(4)(IndexedSeq[Any]())
  var storeI = 3
  private var tupleStored = IndexedSeq[Any]()

  private var groupMap = Map[ListBuffer[Any], ListBuffer[Tuple]]()
  private var groups = ArrayBuffer[ListBuffer[Any]]()
  private var groupIndex = 0
  private var emptyCase = false

  private def getNextSingle(op : Operator): Tuple = {
    storeI += 1
    if (storeI >= blockSize) {
      val block = op.next()
      if (block == null || block.length == 0 || block(0).length == 0)
        return null
      blockStored = Array.fill(4)(IndexedSeq[Any]())
      for (i <- 0 until block.length) {
        blockStored(i) = block(i)
      }
      storeI = 0
    }
    if (blockStored(storeI) == null ||  blockStored(storeI).length == 0 )
      return null
    //println(blockStored(storeI))
    blockStored(storeI)
  }

  private def nextSingle(): Tuple = {
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

  override def open(): Unit = {
    //println("aggregate open")
    input.open()
    var tuple = getNextSingle(input)
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
      tuple = getNextSingle(input)
    }
  }

  override def next(): Block = {
    var t = IndexedSeq[Any]()
    if (tupleStored != null && tupleStored.length > 0)
      t = tupleStored
    else
      t = nextSingle()
    if (t == null)
      return IndexedSeq[IndexedSeq[Any]]()
    var tI = 0
    var result = ListBuffer[Tuple]()
    while (t != null && tI < blockSize) {
      result += t
      t = nextSingle()
      tI += 1
    }
    if (t != null)
      tupleStored = t.toIndexedSeq
    else
      tupleStored = null
    //println(result.toIndexedSeq)
    result.toIndexedSeq
  }

  override def close(): Unit = {
    //println("project close")
    input.close()
  }
}
