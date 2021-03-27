package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode
import collection.JavaConverters._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  var blockStored = Array.fill(4)(IndexedSeq[Any]())
  var storeI = 3
  private var tupleStored = IndexedSeq[Any]()

  private var rowsOffsetted = 0
  private var rowsFetched = 0
  private var tuples = ArrayBuffer[Tuple]()
  private var tupleIndex = 0
  private var chosenIndex = 0
  private val fieldCollations : List[RelFieldCollation] = collation.getFieldCollations.asScala.toList
  private val offsetInt = {
    if (offset == null)
      0
    else
      offset.toString.toInt
  }
  private val fetchInt = {
    if (fetch == null)
      Int.MaxValue
    else
      fetch.toString.toInt
  }

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
    if (tuples.isEmpty) {
      //println("sort done")
      close()
      return null
    }
    if (rowsFetched >= fetchInt) {
      //println("sort done")
      close()
      return null
    }
    if (tupleIndex >= tuples.length) {
      val result = tuples(chosenIndex)
      tuples.remove(chosenIndex)
      tupleIndex = 0
      chosenIndex = 0
      if (rowsOffsetted >= offsetInt) {
        rowsFetched += 1
        //println(result)
        return result
      }
      rowsOffsetted += 1
      return nextSingle()
    }
    var bool = true
    var exit = false
    var fIndex = 0
    while (fIndex < fieldCollations.length && bool && !exit) {
      val fieldCollation = fieldCollations(fIndex)
      val currentField = tuples(chosenIndex)(fieldCollation.getFieldIndex).asInstanceOf[Comparable[Any]]
      val newField = tuples(tupleIndex)(fieldCollation.getFieldIndex).asInstanceOf[Comparable[Any]]
      if (fieldCollation.direction.isDescending) {
        if (newField.compareTo(currentField) < 0 ) {
          bool = false
        } else if (newField.compareTo(currentField) > 0 )  {
          exit = true
        }
      } else {
        if (newField.compareTo(currentField) > 0 ) {
          bool = false
        } else if (newField.compareTo(currentField) < 0 ){
          exit = true
        }
      }
      fIndex += 1
    }
    if (bool) {
      //println("switched")
      chosenIndex = tupleIndex
    }
    tupleIndex += 1
    nextSingle()
  }

  override def open(): Unit = {
    //println("sort open")
    input.open()
    var currentTuple = getNextSingle(input)
    while (currentTuple != null) {
      tuples += currentTuple
      currentTuple = getNextSingle(input)
    }
    //println("tuples")
    //println(tuples)
    //println("---")
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

  override def close(): Unit = input.close()
}
