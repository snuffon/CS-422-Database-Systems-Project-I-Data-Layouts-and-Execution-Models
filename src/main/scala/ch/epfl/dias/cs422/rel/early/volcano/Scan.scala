package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.store.{PAXStore, ColumnStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected val scannable: Store = tableToStore(table.unwrap(classOf[ScannableTable]))
  private var index = -1
  private var paxIndex = 0
  private var subPaxIndex = -1

  override def open(): Unit = {
    index = -1
    paxIndex = 0
    subPaxIndex = -1
  }

  override def next(): Tuple = {
    index += 1
    if (index < scannable.getRowCount) {
      scannable match {
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

  override def close(): Unit = {
    //println("scan close")
    index = -1
    paxIndex = 0
    subPaxIndex = -1
  }
}
