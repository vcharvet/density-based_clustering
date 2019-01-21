package  org.local.clustering

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

  implicit val ss = SparkSession.builder
    .appName("clustering.CoreDistance Unit Test")
    .master("local[1]")
    .getOrCreate()

  import ss.implicits._
  def df: DataFrame =
    ss.sparkContext.parallelize(Seq(
      (0l, -3D, 2D),
      (1l,-3D, 3D),
      (2l, -4D, 2D),
      (3l, 3D, 2D),
      (4l, 5D, 2D),
      (5l, 3D, 3D)))
    .toDF("id", "x", "y")
    .cache()

  val assembler = new VectorAssembler()
    .setInputCols(Array("x", "y"))
    .setOutputCol("features")

  val df_features: DataFrame = assembler.transform(df)

  val idCol = "id"
  val featureCol = "features"

  val neighbors = new Neighbors(2)

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

  val expectedDF = ss.sparkContext.parallelize(Seq(
    (0l, 1l, 1D),
    (0l, 2l, 1D),
    (0l, 3l, 36D),
    (0l, 4l, 64D),
    (0l, 5l, 37D),
    (1l, 2l, 2D),
    (1l, 3l, 37D),
    (1l, 4l, 65D),
    (1l, 5l, 36D),
    (2l, 3l, 49D),
    (2l, 4l, 81D),
    (2l, 5l, 50D),
    (3l, 4l, 4D),
    (3l, 5l, 1D),
    (4l, 5l, 5D)))
    .toDF("id", "id_2", "distance")


  "pointWise distance computation" should "yield" in {
    val computedDF = neighbors.pointWiseDistance(df_features, "id", "features", Vectors.sqdist)

    assertResult(expectedDF.collect())(computedDF.select("id", "id_2", "distance").collect())
  }

  //TODO implement test for kNearestNeighbor
	"core distance with aggregator" should "yield" in {
	  val distanceNoFilter = neighbors.pointWiseDistance(df_features, "id", "features", Vectors.sqdist, false)
	  val computedKNN = neighbors.coreDistance(distanceNoFilter, "id", "id_2", "distance")(ss)
    println("exact doreDistance df:")
     computedKNN.show()
     assertResult(Array[Double](1, 2, 2, 4, 5, 5))(computedKNN.select("coreDistance").collect().map(_.get(0)))
	}

	"approximate core distance" should "yield" in{
	  val computedCD = neighbors.approximateCoreDistance(df_features, idCol, featureCol, 1l, 20d, 5d, Some(Vectors.sqdist))

    println("approximated core distance df:")
    computedCD.show()
	  assertResult(Array[Double](1, 2, 2, 4, 5, 5))(computedCD.select("coreDistance").collect().map(_.get(0)))
	}
}



