package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.ListBuffer

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  lazy val e0: Tuple => Any = eval(condition, input.getRowType)

  override def execute(): IndexedSeq[Column] = {
    val columns = input.execute()
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
}
