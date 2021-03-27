package ch.epfl.dias.cs422.rel.late.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluator, LazyEvaluatorAccess}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  private var l = ListBuffer[Long => Any]()
  val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)

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

  var returnR = IndexedSeq[IndexedSeq[Any]]()
  val columns = getColumns(input)
  if (columns.length == 0)
    returnR = IndexedSeq[IndexedSeq[Any]]()
  else {
    var result = Array.fill(rowType.getFieldCount)(ListBuffer[Any]())
    for (r <- 0 until columns(0).length) {
      var t = ListBuffer[Any]()
      for (c <- 0 until columns.length) {
        t += columns(c)(r)
      }
      val tuple = evaluator(t.toIndexedSeq)
      for (c <- 0 until tuple.length) {
        result(c) += tuple(c)
      }
    }
    var cs = ListBuffer[IndexedSeq[Any]]()
    for (c <- 0 until result.length) {
      if (result(c).length > 0)
        cs += result(c).toIndexedSeq
    }
    val cs0 = cs.toIndexedSeq
    for (c <- cs0) {
      val f: (Long => Any) = long => c(long.toInt)
      l += f
    }
    var result0 = ListBuffer[IndexedSeq[Long]]()
    for (i <- 0 until cs0(0).length)
      result0 += List(i.toLong).toIndexedSeq
    returnR = result0.toIndexedSeq
  }

  private lazy val evals = {
    val access = new LazyEvaluatorAccess(l.toList)
    var list = ListBuffer[Int]()
    for (i <- 0 until l.length)
      list += i
    indices(list.toIndexedSeq, rowType, access)
  }

  override def execute(): IndexedSeq[Column] = {
    returnR
  }

  override def evaluators(): LazyEvaluator = evals
}
