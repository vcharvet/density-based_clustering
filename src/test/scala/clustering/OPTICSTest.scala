package org.local.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FlatSpec

class OPTICSTest extends FlatSpec{
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	implicit val ss = SparkSession.builder
		.appName("clustering.OPTICS Unit Test")
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



	val expected = ss.sparkContext.parallelize(Seq(
		(0l, 0l),
		(1l, 0l),
		(2l, 0l),
		(3l, 3l),
		(4l, 3l),
		(5l, 3l)))
		.toDF()
  	.cache()

//	"clustering OPTICS with kruskal" should "yield" in {
//		val t0 = System.nanoTime
//		val optics = new OPTICS(30D, 2, "id", Vectors.sqdist, "kruskal", "exact", "features")
//
//		val computed = optics.run(dfFeatures)
//		info(s"OPTICS with kruskal ran in ${(System.nanoTime - t0) / 1e9d}")
//
//		assertResult(expected.collect())(computed.collect())
//	}

//	"clustering OPTICS with exact distances and prim" should "yield" in {
//		val t0 = System.nanoTime
//		val optics = new OPTICS(30D, 2, "id", Some(Vectors.sqdist), "prim", "exact", "features")
//
//		val computed = optics.run(dfFeatures)
//
//		info(s"ran in ${(System.nanoTime - t0) / 1e9d}")
//		assertResult(expected.collect())(computed.collect())
//	}

	"clustering OPTICS with exact distances and distributed MST" should "yield" in {
		val t0 = System.nanoTime
		val optics = new OPTICS(30D, 2, "id", Some(Vectors.sqdist), "distributed", "exact",
			Some(2), "features")
		val computed = optics.run(dfFeatures)

		info(s"ran in ${(System.nanoTime - t0) / 1e9d}")
		assertResult(expected.collect())(computed.collect())
	}

//	"clustering OPTICS with approximate distances and distributed MST" should "yield" in {
//		val t0 = System.nanoTime
//		val optics = new OPTICS(30D, 2, "id", Some(Vectors.sqdist), "distributed", "lsh",
//			2, "features")
//		val computed = optics.run(dfFeatures)
//
//		info(s"Ran in ${(System.nanoTime - t0) / 1e9d}")
//		assertResult(expected.collect())(computed.collect())
//	}
}
