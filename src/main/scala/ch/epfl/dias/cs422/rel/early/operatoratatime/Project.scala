package ch.epfl.dias.cs422.rel.early.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {

  val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)
  override def execute(): IndexedSeq[Column] = {
    val columns = input.execute()
    if (columns.length == 0)
      return IndexedSeq[IndexedSeq[Any]]()
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
    cs.toIndexedSeq
  }
}
