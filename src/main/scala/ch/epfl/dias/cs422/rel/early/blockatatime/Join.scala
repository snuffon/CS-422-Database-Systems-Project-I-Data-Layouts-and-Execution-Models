package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  var blockStoredL = Array.fill(4)(IndexedSeq[Any]())
  var storeIL = 3
  var blockStoredR = Array.fill(4)(IndexedSeq[Any]())
  var storeIR = 3
  private var tupleStored = IndexedSeq[Any]()

  private var leftTuples = ArrayBuffer[Tuple]()
  private var groupMap = Map[ListBuffer[Any], ArrayBuffer[Tuple]]()
  private var groups = ArrayBuffer[ListBuffer[Any]]()
  private var leftIndex = 0
  private var rightIndex = 0
  private var leftKeys = ArrayBuffer[Int]()
  private var rightKeys = ArrayBuffer[Int]()

  private def getNextSingleLeft(op: Operator): Tuple = {
    storeIL += 1
    if (storeIL >= blockSize) {
      val block = op.next()
      if (block == null || block.length == 0 || block(0).length == 0)
        return null
      blockStoredL = Array.fill(4)(IndexedSeq[Any]())
      for (i <- 0 until block.length) {
        blockStoredL(i) = block(i)
      }
      storeIL = 0
    }
    if (blockStoredL(storeIL) == null ||  blockStoredL(storeIL).length == 0 )
      return null
    //println(blockStored(storeI))
    blockStoredL(storeIL)
  }

  private def getNextSingleRight(op: Operator): Tuple = {
    storeIR += 1
    if (storeIR >= blockSize) {
      val block = op.next()
      if (block == null || block.length == 0 || block(0).length == 0)
        return null
      blockStoredR = Array.fill(4)(IndexedSeq[Any]())
      for (i <- 0 until block.length) {
        blockStoredR(i) = block(i)
      }
      storeIR = 0
    }
    if (blockStoredR(storeIR) == null ||  blockStoredR(storeIR).length == 0 )
      return null
    //println(blockStored(storeI))
    blockStoredR(storeIR)
  }

  private def nextSingle(): Tuple = {
    if (leftIndex >= leftTuples.length) {
      //println("join done")
      close()
      return null
    }
    var groupLeft = ListBuffer[Any]()
    for (i <- 0 until getLeftKeys.length) {
      groupLeft += leftTuples(leftIndex)(getLeftKeys(i))
    }
    if (!groupMap.contains(groupLeft)){
      leftIndex += 1
      return nextSingle()
    }
    val result = leftTuples(leftIndex) ++ groupMap(groupLeft)(rightIndex)
    rightIndex += 1
    if (rightIndex >= groupMap(groupLeft).length) {
      leftIndex += 1
      rightIndex = 0
    }
    result
  }

  override def open(): Unit = {
    left.open()
    var currentTuple = getNextSingleLeft(left)
    while (currentTuple != null) {
      leftTuples += currentTuple
      currentTuple = getNextSingleLeft(left)
    }
    right.open()
    currentTuple = getNextSingleRight(right)
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
      currentTuple = getNextSingleRight(right)
    }
    for (i <- 0 until getLeftKeys.length) {
      leftKeys += getLeftKeys(i)
      rightKeys += getRightKeys(i)
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
    left.close()
    right.close()
    leftTuples = ArrayBuffer[Tuple]()
    leftIndex = 0
    rightIndex = 0
    leftKeys = ArrayBuffer[Int]()
    rightKeys = ArrayBuffer[Int]()
  }
}
