package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  private var l = ListBuffer[Long => Any]()

  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    store match {
      case row: RowStore => {
        if (row.getRowCount == 0)
          return IndexedSeq[IndexedSeq[Any]]()
        var columns = ArrayBuffer[ListBuffer[Any]]()
        for (i <- 0 until table.getRowType.getFieldCount.toInt)
          columns += ListBuffer[Any]()
        for (r <- 0 until row.getRowCount.toInt) {
          val tuple = row.getRow(r.toInt)
          for (c <- 0 until tuple.length) {
            columns(c) += tuple(c)
          }

        }
        var array = ArrayBuffer[IndexedSeq[Any]]()
        for (c <- 0 until columns.length) {
          array += columns(c).toIndexedSeq
        }
        val cs = array.toIndexedSeq
        for (c <- cs) {
          val f : (Long => Any) = long => c(long.toInt)
          l += f
        }
        var result = ListBuffer[IndexedSeq[Long]]()
        for (i <- 0 until cs(0).length)
          result += List(i.toLong).toIndexedSeq
        result.toIndexedSeq
      }
      case cStore: ColumnStore => {
        var columns = ArrayBuffer[IndexedSeq[Any]]()
        if (cStore.getRowCount == 0)
          return columns.toIndexedSeq
        for (i <- 0 until table.getRowType.getFieldCount.toInt) {
          columns += cStore.getColumn(i)
        }
        val cs = columns.toIndexedSeq
        for (c <- cs) {
          val f : (Long => Any) = long => c(long.toInt)
          l += f
        }
        var result = ListBuffer[IndexedSeq[Long]]()
        for (i <- 0 until cs(0).length)
          result += List(i.toLong).toIndexedSeq
        result.toIndexedSeq
      }
      case pStore: PAXStore => {
        if (pStore.getRowCount == 0)
          return IndexedSeq[IndexedSeq[Any]]()
        var columns = ArrayBuffer[ListBuffer[Any]]()
        for (i <- 0 until table.getRowType.getFieldCount.toInt)
          columns += ListBuffer[Any]()
        var index = 0
        var counter = 0
        while (counter < pStore.getRowCount) {
          val currentPax = pStore.getPAXPage(index)
          for (i <- 0 until columns.length) {
            for (elem <- currentPax(i))
              columns(i) += elem
          }
          index += 1
          counter += currentPax(0).length
        }
        var array = ArrayBuffer[IndexedSeq[Any]]()
        for (c <- 0 until columns.length) {
          array += columns(c).toIndexedSeq
        }
        val cs = array.toIndexedSeq
        for (c <- cs) {
          val f : (Long => Any) = long => c(long.toInt)
          l += f
        }
        var result = ListBuffer[IndexedSeq[Long]]()
        for (i <- 0 until cs(0).length)
          result += List(i.toLong).toIndexedSeq
        result.toIndexedSeq
      }
    }
  }

  private lazy val evals = {
    new LazyEvaluatorAccess(l.toList)
  }

  override def evaluators(): LazyEvaluatorAccess = evals
}
