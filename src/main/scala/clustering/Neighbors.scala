package org.local.clustering

import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import breeze.linalg.{DenseMatrix, DenseVector => BreezeVector}
import org.apache.spark.sql.expressions.UserDefinedFunction



/* class to fetch nearest neigbors and computing disimilarity matrix
   */
class Neighbors(neighbors: Int){
  /** compute similarity distance between samples in df
    *
    *
    * @param df DataFrame containing client features and ids ['id', 'features'] like
    * @param cols string of the feature column, usually "features"
    * @return breeze matrix containing similarity distances, the matrix is strictly
    *         triangular superior since distance is only computed for indices i < j
    *
    */
  def dissimilarityMatrix(df: DataFrame, idCol: String, featureCols: String,
    distance: (DenseVector, DenseVector) => Double)(ss: SparkSession): DenseMatrix[Double] = {
    val n = df.count().toInt //computationnaly heavy

    val df2 = df.select(idCol, featureCols)
      .withColumnRenamed(idCol, idCol + "_2")
      .withColumnRenamed(featureCols, featureCols + "_2")

    val cartesian = df.select(idCol, featureCols).crossJoin(df2)
      .filter(col(idCol) < col(idCol + "_2") )

    val dissimilarityMatrix = new MatrixAccumulator(n, n)
    ss.sparkContext.register(dissimilarityMatrix, "dissimMatrix1")
    //TODO try without accumlator: https://stackoverflow.com/questions/37012059/how-to-find-the-nearest-neighbors-of-1-billion-records-with-spark
    cartesian.foreach(row => row match {
      case Row(i: Long, vector1: DenseVector, j: Long, vector2: DenseVector)
        => dissimilarityMatrix.add(i.toInt , j.toInt, distance(vector1, vector2))
    })
    dissimilarityMatrix.value
  }

  /** computes pointwise distance between each pair of points
    *
    * @param df
    * @param idCol
    * @param featureCol
    * @param distance
    * @param ss
    * @return
    */
  def pointWiseDistance(df: DataFrame, idCol: String, featureCol: String,
    distance: (DenseVector, DenseVector) => Double, filter: Boolean=true): DataFrame = {
//    import ss.implicits._
    val df2 = df.select(idCol, featureCol)
      .withColumnRenamed(idCol, idCol + "_2")
      .withColumnRenamed(featureCol, featureCol + "_2")

    val cartesian = df.select(idCol, featureCol)
      .crossJoin(df2)
      .filter(if (filter) col(idCol) < col(idCol + "_2") else col(idCol) =!= col(idCol + "_2"))

    val distanceDF = cartesian
      .withColumn("distance", this.distanceUDF(distance)(col(featureCol), col(featureCol + "_2")))

    distanceDF
  }

  def distanceUDF(distance: (DenseVector, DenseVector) => Double): UserDefinedFunction =
    udf((vector1: DenseVector, vector2: DenseVector) => distance(vector1, vector2))

  /** fetches kth nearest neighbor for each point in df[idCol1]
    * The aggregator returns DaatFrame[i, Map(kNN(i), CD(i)]
    *
    * @param df
    * @param idCol1
    * @param idCol2
    * @param distanceCol
    * @param ss
    * @return
    */
  def kNearestNeighbor(df: DataFrame, idCol1: String, idCol2: String,
    distanceCol: String)(implicit ss: SparkSession) : DataFrame = {

    val nnAgg = new NearestNeighborAgg(
      this.neighbors, idCol1, distanceCol)

    val dfGroup = df
      .select(idCol1, idCol2, distanceCol)
      .groupBy(idCol1)
      .agg(nnAgg.toColumn)
      .alias("kNNDistance")
      .orderBy(idCol1)  // ?

    dfGroup
  }

}


