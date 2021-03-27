package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  var blockStored = Array.fill(4)(IndexedSeq[Any]())
  var storeI = 3
  private var tupleStored = IndexedSeq[Any]()


  def getNextSingle(op : Operator): Tuple = {
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

  def nextSingle(): Tuple = {

    var currentTuple = getNextSingle(input)
    if (currentTuple == null) {
      close()
      //println("filter done")
      return null
    }
    if (e(currentTuple) == true)
      return currentTuple
    nextSingle()
  }

  override def open(): Unit = input.open()


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
    //println("filter close")
    input.close()
  }
}
