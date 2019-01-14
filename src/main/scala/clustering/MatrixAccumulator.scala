package org.local.clustering

import breeze.linalg.DenseMatrix
import org.apache.spark.util.AccumulatorV2


/** accumulator class for dense matrices
  *
  * @param rows
  * @param cols
  */
class MatrixAccumulator(rows: Int, cols: Int) extends AccumulatorV2[(Int, Int, Double),
  DenseMatrix[Double]]{
  private val matrix: DenseMatrix[Double] = DenseMatrix.zeros(rows, cols)

  def value: DenseMatrix[Double] = matrix

  def isZero: Boolean = (matrix == DenseMatrix.zeros[Double](rows, cols))

  def add(tuple: (Int, Int, Double)): Unit = matrix.update(tuple._1, tuple._2, tuple._3)

  def reset(): Unit = {
    for (keys <- matrix.keysIterator) matrix.update(keys, 0D)
  }

  def copy(): MatrixAccumulator = this

  def merge(other: AccumulatorV2[(Int, Int, Double), DenseMatrix[Double]]): Unit = {
    for (iter <- other.value.activeIterator){
      matrix.update(iter._1, iter._2 + matrix.apply(iter._1))
    }
  }

}
