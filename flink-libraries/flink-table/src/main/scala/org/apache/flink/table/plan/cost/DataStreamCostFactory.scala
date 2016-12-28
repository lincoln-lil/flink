package org.apache.flink.table.plan.cost

import org.apache.calcite.plan.{RelOptCost, RelOptCostFactory}

class DataStreamCostFactory extends RelOptCostFactory {

  override def makeCost(dElementRate: Double, dCpu: Double, dIops: Double): RelOptCost = {
    new DataStreamCost(dElementRate, dCpu, dIops)
  }

  override def makeHugeCost: RelOptCost = {
    DataStreamCost.Huge
  }

  override def makeInfiniteCost: RelOptCost = {
    DataStreamCost.Infinity
  }

  override def makeTinyCost: RelOptCost = {
    DataStreamCost.Tiny
  }

  override def makeZeroCost: RelOptCost = {
    DataStreamCost.Zero
  }
}
