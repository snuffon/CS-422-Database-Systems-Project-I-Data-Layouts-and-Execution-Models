package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

import collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

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

  override def open(): Unit = {
    //println("sort open")
    input.open()
    var currentTuple = input.next()
    while (currentTuple != null) {
      tuples += currentTuple
      currentTuple = input.next()
    }
    //println("tuples")
    //println(tuples)
    //println("---")
  }

  override def next(): Tuple = {
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
      return next()
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
    next()
  }

  override def close(): Unit = input.close()
}
