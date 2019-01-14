package org.local.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FlatSpec


class NeighborsAgregatorTest extends FlatSpec {
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	val ss = SparkSession.builder
		.appName("clustering.CoreDistance Unit Test")
		.master("local[1]")
		.getOrCreate()

	import ss.implicits._

	val ds = ss.sparkContext.parallelize(Seq(
		(3l, 4l, 4d),
		(3l, 5l, 1d),
		(4l, 3l, 4d),
		(4l, 5l, 5d),
		(5l, 3l, 1d),
		(5l, 4l, 5d)))
		.toDF("i", "j", "distIJ")
//  	.map(row => (row._1, GroupRow(row._2, row._3)))
//  	.toDF("i", "distIJ")

	ds.show()



	"Sorting distances" should "yield" in {
		val sequence = Seq[(Long, Double)]( (0l,1d), (1l,2), (2l,3), (3l,4), (4l,6), (5l,7))
		val udfAgg = new NearestNeighborAgg(2, "j", "distIJ")
		val computed = udfAgg.sortInsert(sequence, (6l,5d))

		val expected = Seq((0l,1d), (1l,2), (2l,3), (3l,4), (6l,5d), (4l,6), (5l,7))

		assertResult(expected)(computed)
	}


	"Aggregator on 2 nearest neighbors" should "yield" in{
//		println("Test not yet implemented")
		val udfAgg = new NearestNeighborAgg(2, "j", "distIJ")


		val computed = ds.groupBy($"i").agg(udfAgg.toColumn as "coreDist")
			.orderBy("i")

		println("Computed: ds")
		computed.show()

		val expected = ss.sparkContext.parallelize(Seq(
			(3l,(4l, 4d)),
			(4l, (5l, 5d)),
			(5l, (4l, 5d)))).toDF("j", "2NNeighbor")

		assertResult(expected.collect())(computed.collect())
	}
}