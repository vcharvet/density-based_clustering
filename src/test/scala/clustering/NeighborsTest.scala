package  clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql._
import org.scalatest.FlatSpec
import breeze.linalg.{DenseMatrix => BreezeMatrix}

import scala.math.sqrt




class NeighborsTest extends FlatSpec {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val ss = SparkSession.builder
    .appName("clustering.CoreDistance Unit Test")
    .master("local[1]")
    .getOrCreate()

  import ss.implicits._
  def df: DataFrame =
    ss.sparkContext.parallelize(Seq(
      (1D, -3D, 2D),
      (2D,-3D, 3D),
      (3D, -4D, 2D),
      (4D, 3D, 2D),
      (5D, 5D, 2D),
      (6D, 3D, 3D)))
    .toDF("id", "x", "y")
    .cache()

  val assembler = new VectorAssembler()
    .setInputCols(Array("x", "y"))
    .setOutputCol("features")

  val df_features: DataFrame = assembler.transform(df)

  val idCol = "id"
  val featureCol = "features"

  val neighbors = new clustering.Neighbors(3)

  "dissimilarity matrix" should "yield" in {
    val computedMatrix = neighbors.dissimilarityMatrix(df_features, idCol, featureCol,
      Vectors.sqdist)(ss)

    val expectedMatrix = BreezeMatrix(
      (0D, 1D, 1D, 36D, 64D, 37D),
      (0D, 0D, 2D, 37D, 65D, 36D),
      (0D, 0D, 0D, 49D, 81D, 50D),
      (0D, 0D, 0D, 0D, 4D, 1D),
      (0D, 0D, 0D, 0D, 0D, 5D),
      (0D, 0D, 0D, 0D, 0D, 0D)
    )

    assertResult(expectedMatrix)(computedMatrix)
  }

  "pointWise distance computation" should "yield" in {
    val computedDF = neighbors.pointWiseDistance(df_features, "id", "features", Vectors.sqdist)

    val expectedDF = ss.sparkContext.parallelize(Seq(
      (1D, 2D, 1D),
      (1D, 3D, 1D),
      (1D, 4D, 36D),
      (1D, 5D, 64D),
      (1D, 6D, 37D),
      (2D, 3D, 2D),
      (2D, 4D, 37D),
      (2D, 5D, 65D),
      (2D, 6D, 36D),
      (3D, 4D, 49D),
      (3D, 5D, 81D),
      (3D, 6D, 50D),
      (4D, 5D, 4D),
      (4D, 6D, 1D),
      (5D, 6D, 5D)))
      .toDF("id", "id_2", "distance")

    assertResult(expectedDF.collect())(computedDF.select("id", "id_2", "distance").collect())
  }
}


class CoreDistanceTest extends FlatSpec {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val ss = SparkSession.builder
    .appName("clustering.CoreDistance Unit Test")
    .master("local[1]")
    .getOrCreate()

  import ss.implicits._

  def df: DataFrame =
    ss.sparkContext.parallelize(Seq(
      (1D, -3D, 2D),
      (2D,-3D, 3D),
      (3D, -4D, 2D),
      (4D, 3D, 2D),
      (5D, 5D, 2D),
      (6D, 3D, 3D)))
    .toDF("id", "x", "y")
    .cache()




  val assembler = new VectorAssembler()
    .setInputCols(Array("x", "y"))
    .setOutputCol("features")

  val df_features: DataFrame = assembler.transform(df)

  val LSHModel = new BucketedRandomProjectionLSH()
    .setNumHashTables(4)
    .setInputCol("features")
    .setBucketLength(3D)
    .fit(df_features)

  val CD = new clustering.CoreDistance()
  val minPts = 3
//  CD.setMinPts(3)

  val point1 = new DenseVector(Array(-3D, 2D))

  "Core Distance of point1 should" should "be" in {
    val computed1 = CD.computeCoreDistance(point1, df_features, LSHModel, minPts)(ss)
    val actual1 = 1D

    assertResult(actual1)(computed1)
  }

  val point2 = new DenseVector(Array(-3D, 3D))

  "Core Distance of point2 should" should "be" in {
    val computed2 = CD.computeCoreDistance(point2, df_features, LSHModel, minPts)(ss)
    val actual2 = sqrt(2)

    assertResult(actual2)(computed2)
  }

  val point4 = new DenseVector(Array(3D, 2D))

  "Core Distance of point4 should" should "be" in {
    val computed4 = CD.computeCoreDistance(point4, df_features, LSHModel, minPts)(ss)
    val actual4 = 2D

    assertResult(actual4)(computed4)
  }

  val point5 = new DenseVector(Array(5D, 2D))

  "Core Distance of point5 should" should "be" in {
    val computed5 = CD.computeCoreDistance(point5, df_features, LSHModel, minPts)(ss)
    val actual5 = sqrt(5)

    assertResult(actual5)(computed5)
  }

  val point6 = new DenseVector(Array(3D, 3D))

  "Core Distance of point6 should" should "be" in {
    val computed6 = CD.computeCoreDistance(point6, df_features, LSHModel, minPts)(ss)
    val actual6 = sqrt(5)

    assertResult(actual6)(computed6)
  }
  def actualTot: DataFrame = ss.sparkContext.parallelize(Seq(
    (1D, 1),
    (2D, sqrt(2)),
    (3D, sqrt(2)),
    (4D, 2),
    (5D, sqrt(5)),
    (6D, sqrt(5))))
    .toDF("id", "core_distance")

  "Core distances mapping on whole df" should "yield" in {
//    val computedTot: DataFrame = df_features.select("id", "features")
//      .withColumn("computed_core_distance",
//        CD.coreDistanceUDF(df_features, LSHModel, minPts)(ss)(df_features("features")))

//    val computedTot: DataFrame = df_features.select("id", "features")
//        .map {row: Row =>
//          val feature = row.getAs[DenseVector]("features")
//          val id = row.getAs[Double]("id")
//          val coreDist = CD.computeCoreDistance(feature, df_features, LSHModel, minPts)(ss)
//          Row(id, coreDist)
//        }
    //@TODO complete test
        println("Test not yet implanted")

//        .map(row => CD.computeCoreDistance(row.getAs("features"), df_features.select("features"),
//          LSHModel, 2)(ss))
      assertResult(true)(false)
//    assertResult(actualTot("core_distance"))(computedTot("computed_core_distance"))
  }
}

