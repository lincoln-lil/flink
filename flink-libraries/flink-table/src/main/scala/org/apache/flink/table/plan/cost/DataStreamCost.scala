package org.apache.flink.table.plan.cost

import org.apache.calcite.plan.{RelOptUtil, RelOptCostFactory, RelOptCost}
import org.apache.calcite.util.Util

class DataStreamCost(val elementRate: Double, val cpu: Double, val iops: Double) extends RelOptCost {

  def getCpu: Double = cpu

  def isInfinite: Boolean = {
    (this eq DataStreamCost.Infinity) ||
      (this.elementRate == Double.PositiveInfinity) ||
      (this.cpu == Double.PositiveInfinity) ||
      (this.iops == Double.PositiveInfinity)
  }

  def getIo: Double = iops

  def isLe(other: RelOptCost): Boolean = {
    val that: DataStreamCost = other.asInstanceOf[DataStreamCost]
    (this eq that) ||
      (this.iops < that.iops) ||
      (this.iops == that.iops && this.cpu < that.cpu) ||
      (this.iops == that.iops && this.cpu == that.cpu && this.elementRate < that.elementRate)
  }

  def isLt(other: RelOptCost): Boolean = {
    isLe(other) && !(this == other)
  }

  def getRows: Double = elementRate

  override def hashCode: Int = Util.hashCode(elementRate) + Util.hashCode(cpu) + Util.hashCode(iops)

  def equals(other: RelOptCost): Boolean = {
    (this eq other) ||
      other.isInstanceOf[DataStreamCost] &&
        (this.elementRate == other.asInstanceOf[DataStreamCost].elementRate) &&
        (this.cpu == other.asInstanceOf[DataStreamCost].cpu) &&
        (this.iops == other.asInstanceOf[DataStreamCost].iops)
  }

  def isEqWithEpsilon(other: RelOptCost): Boolean = {
    if (!other.isInstanceOf[DataStreamCost]) {
      return false
    }
    val that: DataStreamCost = other.asInstanceOf[DataStreamCost]
    (this eq that) ||
      ((Math.abs(this.elementRate - that.elementRate) < RelOptUtil.EPSILON) &&
        (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON) &&
        (Math.abs(this.iops - that.iops) < RelOptUtil.EPSILON))
  }

  def minus(other: RelOptCost): RelOptCost = {
    if (this eq DataStreamCost.Infinity) {
      return this
    }
    val that: DataStreamCost = other.asInstanceOf[DataStreamCost]
    new DataStreamCost(this.elementRate - that.elementRate, this.cpu - that.cpu, this.iops - that.iops)
  }

  def multiplyBy(factor: Double): RelOptCost = {
    if (this eq DataStreamCost.Infinity) {
      return this
    }
    new DataStreamCost(elementRate * factor, cpu * factor, iops * factor)
  }

  def divideBy(cost: RelOptCost): Double = {
    val that: DataStreamCost = cost.asInstanceOf[DataStreamCost]
    var d: Double = 1
    var n: Double = 0
    if ((this.elementRate != 0) && !this.elementRate.isInfinite &&
      (that.elementRate != 0) && !that.elementRate.isInfinite)
    {
      d *= this.elementRate / that.elementRate
      n += 1
    }
    if ((this.cpu != 0) && !this.cpu.isInfinite && (that.cpu != 0) && !that.cpu.isInfinite) {
      d *= this.cpu / that.cpu
      n += 1
    }
    if ((this.iops != 0) && !this.iops.isInfinite && (that.iops != 0) && !that.iops.isInfinite) {
      d *= this.iops / that.iops
      n += 1
    }
    if (n == 0) {
      return 1.0
    }
    Math.pow(d, 1 / n)
  }

  def plus(other: RelOptCost): RelOptCost = {
    val that: DataStreamCost = other.asInstanceOf[DataStreamCost]
    if ((this eq DataStreamCost.Infinity) || (that eq DataStreamCost.Infinity)) {
      return DataStreamCost.Infinity
    }
    new DataStreamCost(this.elementRate + that.elementRate, this.cpu + that.cpu, this.iops + that.iops)
  }

  override def toString: String = s"{$elementRate rows, $cpu cpu, $iops io}"

}

object DataStreamCost {

  private[flink] val Infinity = new DataStreamCost(
    Double.PositiveInfinity,
    Double.PositiveInfinity,
    Double.PositiveInfinity)
  {
    override def toString: String = "{inf}"
  }

  private[flink] val Huge = new DataStreamCost(Double.MaxValue, Double.MaxValue, Double.MaxValue) {
    override def toString: String = "{huge}"
  }

  private[flink] val Zero = new DataStreamCost(0.0, 0.0, 0.0) {
    override def toString: String = "{0}"
  }

  private[flink] val Tiny = new DataStreamCost(1.0, 1.0, 1.0) {
    override def toString = "{tiny}"
  }

  val FACTORY: RelOptCostFactory = new DataStreamCostFactory
}

