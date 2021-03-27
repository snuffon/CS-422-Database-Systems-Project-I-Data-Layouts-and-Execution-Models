package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  private var leftTuples = ArrayBuffer[Tuple]()
  private var groupMap = Map[ListBuffer[Any], ArrayBuffer[Tuple]]()
  private var groups = ArrayBuffer[ListBuffer[Any]]()
  private var leftIndex = 0
  private var rightIndex = 0
  private var leftKeys = ArrayBuffer[Int]()
  private var rightKeys = ArrayBuffer[Int]()

  override def open(): Unit = {
    left.open()
    var currentTuple = left.next()
    while (currentTuple != null) {
      leftTuples += currentTuple
      currentTuple = left.next()
    }
    right.open()
    currentTuple = right.next()
    while (currentTuple != null) {
      var group = ListBuffer[Any]()
      for (i <- 0 until getRightKeys.length) {
        group += currentTuple(getRightKeys(i))
      }
      if (!groupMap.contains(group)) {
        groups += group
        groupMap += (group -> ArrayBuffer[Tuple]())
      }
      groupMap(group) = groupMap(group) :+ currentTuple
      currentTuple = right.next()
    }
    for (i <- 0 until getLeftKeys.length) {
      leftKeys += getLeftKeys(i)
      rightKeys += getRightKeys(i)
    }
  }

  override def next(): Tuple = {
    if (leftIndex >= leftTuples.length) {
      println("join done")
      close()
      return null
    }
    var groupLeft = ListBuffer[Any]()
    for (i <- 0 until getLeftKeys.length) {
      groupLeft += leftTuples(leftIndex)(getLeftKeys(i))
    }
    if (!groupMap.contains(groupLeft)){
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

  override def close(): Unit = {
    left.close()
    right.close()
    leftTuples = ArrayBuffer[Tuple]()
    leftIndex = 0
    rightIndex = 0
    leftKeys = ArrayBuffer[Int]()
    rightKeys = ArrayBuffer[Int]()
  }
}
