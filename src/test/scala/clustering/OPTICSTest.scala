package clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FlatSpec

class OPTICSTest extends FlatSpec{
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

	def assembler = new VectorAssembler()
  	.setInputCols(Array("x", "y"))
  	.setOutputCol("features")

val dfFeatures = assembler
  .transform(df)
  .drop("x", "y")

	val optics = new OPTICS(30D, 2, "id", Vectors.sqdist, "features")
//	optics.setDistance(Vectors.sqdist)


	val computed = optics.run(dfFeatures)(ss)

	"clustering OPTICS" should "yield" in {
		val expected = ss.sparkContext.parallelize(Seq(
			(0l, 0l),
			(1l, 0l),
			(2l, 0l),
			(3l, 3l),
			(4l, 3l),
			(5l, 3l)))
			.toDF()
		assertResult(expected.collect())(computed.collect())

	}

}
