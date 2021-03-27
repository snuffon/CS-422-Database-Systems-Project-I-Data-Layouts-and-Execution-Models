package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluatorAccess, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
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

  var leftColumns = getColumns(left)
  var rightColumns = getColumns(right)

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

  private def executeMaterialized(): IndexedSeq[Column] = {
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

  override def execute(): IndexedSeq[Column] = x

  //private lazy val evals = lazyEval(left.evaluators(), right.evaluators(), left.getRowType, right.getRowType)
  private lazy val evals = new LazyEvaluatorAccess(l.toList)

  override def evaluators(): LazyEvaluatorRoot = evals
}
