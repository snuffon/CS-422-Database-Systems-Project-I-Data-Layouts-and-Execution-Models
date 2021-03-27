package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.collection.mutable.ListBuffer

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  private var index = -1
  private var paxIndex = 0
  private var subPaxIndex = -1
  private var tupleStored = IndexedSeq[Any]()

  // similar to volcano next
  def nextSingle(): Tuple = {
    index += 1
    if (index < store.getRowCount) {
      store match {
        case row: RowStore => {
          //println(row.getRow(index))
          return row.getRow(index)
        }
        case column: ColumnStore => {
          val nbr: Int = table.getRowType.getFieldCount
          (0 to nbr - 1).map { field => column.getColumn(field)(index) }
        }
        case pax: PAXStore => {
          subPaxIndex += 1
          if (subPaxIndex >= pax.getPAXPage(paxIndex)(0).length) {
            paxIndex += 1
            subPaxIndex = 0
          }
          val nbr: Int = table.getRowType.getFieldCount

          (0 to nbr - 1).map { field => pax.getPAXPage(paxIndex)(field)(subPaxIndex) }
        }
      }
    } else {
      //println("scan done")
      return null
    }
  }

  override def open(): Unit = {
    index = -1
    paxIndex = 0
    subPaxIndex = -1
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

  override def close(): Unit = {
    //println("scan close")
    index = -1
    paxIndex = 0
    subPaxIndex = -1
  }
}
