package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  private var leftTuples = ArrayBuffer[Tuple]()
  private var groupMap = Map[ListBuffer[Any], ArrayBuffer[Tuple]]()
  private var groups = ArrayBuffer[ListBuffer[Any]]()
  private var leftIndex = 0
  private var rightIndex = 0
  private var leftKeys = ArrayBuffer[Int]()
  private var rightKeys = ArrayBuffer[Int]()
  private var emptyCase = false

  var leftColumns = left.execute()
  var rightColumns = right.execute()

  if (leftColumns == null || leftColumns.length == 0 || leftColumns(0).length == 0 || rightColumns == null || rightColumns.length == 0 || rightColumns(0).length == 0)
    emptyCase = true
  else {
    for (r <- 0 until leftColumns(0).length) {
      var t = ListBuffer[Any]()
      for (c <- 0 until leftColumns.length) {
        t += leftColumns(c)(r)
      }
      leftTuples += t.toIndexedSeq
    }

    for (r <- 0 until rightColumns(0).length) {
      var t = ListBuffer[Any]()
      for (c <- 0 until rightColumns.length) {
        t += rightColumns(c)(r)
      }
      var group = ListBuffer[Any]()
      for (i <- 0 until getRightKeys.length) {
        group += t(getRightKeys(i))
      }

      if (!groupMap.contains(group)) {
        groups += group
        groupMap += (group -> ArrayBuffer[Tuple]())
      }
      groupMap(group) = groupMap(group) :+ t.toIndexedSeq
    }
    for (i <- 0 until getLeftKeys.length) {
      leftKeys += getLeftKeys(i)
      rightKeys += getRightKeys(i)
    }
  }

  // same as volcano
  def next(): Tuple = {
    if (leftIndex >= leftTuples.length) {
      return null
    }
    var groupLeft = ListBuffer[Any]()
    for (i <- 0 until getLeftKeys.length) {
      groupLeft += leftTuples(leftIndex)(getLeftKeys(i))
    }
    if (!groupMap.contains(groupLeft)) {
      leftIndex += 1
      return next()
    }
    val result = leftTuples(leftIndex) ++ groupMap(groupLeft)(rightIndex)
    rightIndex += 1
    if (rightIndex >= groupMap(groupLeft).length) {
      leftIndex += 1
      rightIndex = 0
    }
    result
  }

  override def execute(): IndexedSeq[Column] = {
    var tuples = ArrayBuffer[Tuple]()
    var tuple = next()
    if (tuple == null || emptyCase)
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

}
