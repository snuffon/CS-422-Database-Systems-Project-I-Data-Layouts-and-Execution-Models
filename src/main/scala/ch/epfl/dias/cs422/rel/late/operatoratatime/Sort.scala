package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode
import collection.JavaConverters._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  private var rowsOffsetted = 0
  private var rowsFetched = 0
  private var tuples = ArrayBuffer[Tuple]()
  private var tupleIndex = 0
  private var chosenIndex = 0
  private val fieldCollations: List[RelFieldCollation] = collation.getFieldCollations.asScala.toList
  private var emptyCase = false
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
      tuples += tuple
    }
  }

  // same as volcano
  def next(): Tuple = {
    if (tuples.isEmpty) {
      return null
    }
    if (rowsFetched >= fetchInt) {
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
        if (newField.compareTo(currentField) < 0) {
          bool = false
        } else if (newField.compareTo(currentField) > 0) {
          exit = true
        }
      } else {
        if (newField.compareTo(currentField) > 0) {
          bool = false
        } else if (newField.compareTo(currentField) < 0) {
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

  def executeMaterialized(): IndexedSeq[Column] = {
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
