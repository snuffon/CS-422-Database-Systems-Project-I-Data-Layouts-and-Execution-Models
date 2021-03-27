package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{Evaluator, LazyEvaluatorAccess, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {

  lazy val e0: Tuple => Any = eval(condition, input.getRowType)

  private def executeMaterialized(): IndexedSeq[Column] = {
    val columns = getColumns(input)
    if (columns.length == 0)
      return IndexedSeq[IndexedSeq[Any]]()
    var result = Array.fill(columns.length)(ListBuffer[Any]())
    for (r <- 0 until columns(0).length) {
      var t = ListBuffer[Any]()
      for (c <- 0 until columns.length) {
        t += columns(c)(r)
      }
      if (e0(t.toIndexedSeq) == true) {
        for (c <- 0 until t.length) {
          result(c) += t(c)
        }
      }
    }
    var cs = ListBuffer[IndexedSeq[Any]]()
    for (c <- 0 until result.length) {
      if (result(c).length > 0)
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

  lazy val e: Evaluator = eval(condition, input.getRowType, input.evaluators())

  private lazy val evals = new LazyEvaluatorAccess(l.toList)


  override def evaluators(): LazyEvaluatorRoot = evals
}
