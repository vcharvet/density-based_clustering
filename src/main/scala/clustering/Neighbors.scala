package clustering

import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import breeze.linalg.{DenseMatrix, DenseVector => BreezeVector}
//import clustering.NearestNeighborAgg



/* class to fetch nearest neigbors and computing disimilarity matrix
//TODO change distance to a func, and types of accumulator
   */
class Neighbors(neighbors: Int){
  /** compute similarity distance between samples in df
    *
    * @deprecated as slicing and indexing of the matrix requires ordering of ids
    *             in df, starting from 1 to n-1, were n = number of samples
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
      case Row(i: Double, vector1: DenseVector, j: Double, vector2: DenseVector)
        => dissimilarityMatrix.add(i.toInt - 1, j.toInt - 1, distance(vector1, vector2))
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

  def distanceUDF(distance: (DenseVector, DenseVector) => Double) = //TODO type annotation?
    udf((vector1: DenseVector, vector2: DenseVector) => distance(vector1, vector2))


//  def kNearestNeighbor(df: DataFrame, idCol1: String, icCol2: String,
//    distanceCol: String)(ss: SparkSession) : DataFrame = {
//    ss.sparkContext.udf.register("neighborsAgg", NearestNeighborAgg)
//
//    val dfGroup = df.groupBy(idCol1).agg()
//
//  }

}

//@deprecated
class CoreDistance {
//  var minPts: Int = 2
//  def setMinPts(minPts: Int): Unit = {this.minPts = minPts}
  /** computes the core distance of point x relative to dataset X
    *
    * @param xi
    * @param X
    * @param LSHModel
    * @return
    */
  def computeCoreDistance(xi: DenseVector, X: DataFrame,
                          LSHModel: BucketedRandomProjectionLSHModel, minPts: Int=2)
                         (ss: SparkSession): Double = {
    import ss.implicits._

    val dfNN = LSHModel.approxNearestNeighbors(X, xi, minPts)

    val row: Row = dfNN.agg(max($"distCol")).first()

    row.getAs[Double](0)
  }

  /** udf function that takes vector as input, made to be used in mapping function over
    * a DataFrame
    *
    * @param X
    * @param LSHModel
    * @return
    */
  def coreDistanceUDF(X: DataFrame, LSHModel: BucketedRandomProjectionLSHModel, minPts: Int)
                     (ss: SparkSession) =
    udf((vector: DenseVector) => computeCoreDistance(vector, X, LSHModel, minPts)(ss))
}


