package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def open(): Unit = input.open()

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def next(): Tuple = {
    var currentTuple = input.next()
    if (currentTuple == null) {
      close()
      //println("filter done")
      return null
    }
    if (e(currentTuple) == true)
      return currentTuple
    next()
  }

  override def close(): Unit = {
    //println("filter close")
    input.close()
  }
}
