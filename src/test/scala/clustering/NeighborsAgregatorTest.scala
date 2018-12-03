package clustering

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

	val df = ss.sparkContext.parallelize(Seq(
		(3l, 4l, 4d),
		(3l, 5l, 1d),
		(4l, 3l, 4d),
		(4l, 5l, 5d),
		(5l, 4l, 1d),
		(5l, 4l, 5d)))
  	.toDF("i", "j", "distIJ")

	// group of point 4
	val dfGroup = ss.sparkContext.parallelize(Seq(
		(4l, 4d),
		(5l, 1d)))
  	.toDF("j", "distIJ")

	val grouped_df = df.groupBy("i")

	"Sorting distances" should "yield" in {
		val sequence = Seq(1d, 2, 3, 4, 6, 7)
		val udfAgg = new NearestNeighborAgg(2, "j", "distIJ")
		val computed = udfAgg.sortInsert(sequence, 5d)

		val expected = Seq(1d, 2, 3, 4, 5d, 6, 7)

		assertResult(expected)(computed)
	}

	"Aggregator on small df" should "yield" in {
		val udfAgg = new NearestNeighborAgg(2, "j", "distIJ")
		val resCol = dfGroup.select(udfAgg($"j", $"distIJ"))

		assertResult(Map((4l, 4d)))(resCol.head().get(0))
		}

	"Aggregator on 2 nearest neighbors" should "yield" in{
		val udfAgg = new NearestNeighborAgg(2, "j", "distIJ")

		val computed = grouped_df.agg(udfAgg($"j", $"distIJ").alias("kNeighborDistance") )
  			.orderBy("i")

		val expected = ss.sparkContext.parallelize(Seq(
			(3l, Map[Long, Double]((4l, 4d))),
			(4l, Map[Long, Double]((5l, 5d))),
			(5l, Map[Long, Double]((4l, 5d))))).toDF("j", "2NNeighbor")

		assertResult(expected.collect()  )(computed.collect())
		}
}


class NeighborsAgregator2Test extends FlatSpec {
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
		(5l, 4l, 1d),
		(5l, 4l, 5d)))
  	.map(row => (row._1, GroupRow(row._2, row._3)))
  	.toDF("i", "distIJ")
//  	.map(row => (row.get(0), GroupRow(row.getAs[Long](1), row.getAs[Double](2))))
//  	.withColumnRenamed("_1", "i")
//  	.withColumnRenamed("_2", "distIJ")
  	.as[(Long, GroupRow)]

	ds.show()

	// group of point 4
	val dfGroup = ss.sparkContext.parallelize(Seq(
		(4l, 4d),
		(5l, 1d)))
		.toDF("j", "distIJ")

//	val grouped_df = df.groupBy("i")

	"Sorting distances" should "yield" in {
		val sequence = Seq[(Long, Double)]( (0l,1d), (1l,2), (2l,3), (3l,4), (4l,6), (5l,7))
		val udfAgg = new NearestNeighborAgg2(2)
		val computed = udfAgg.sortInsert(sequence, (6l,5d))

		val expected = Seq((0l,1d), (1l,2), (2l,3), (3l,4), (6l,5d), (4l,6), (5l,7))

		assertResult(expected)(computed)
	}

	"Aggregator on small df" should "yield" in {
		val udfAgg = new NearestNeighborAgg2(2).toColumn
		val resCol = dfGroup.as[GroupRow].select(udfAgg)

		assertResult((4l, 4d))(resCol.head())
	}

	"Aggregator on 2 nearest neighbors" should "yield" in{
//		println("Test not yet implemented")
		val udfAgg = new NearestNeighborAgg2(2)


//		val computed = ds.groupByKey(_._1). //.alias("kNeighborDistance"))
		val computed = ds.groupBy("i").agg(udfAgg.toColumn(col("distIJ")).as("coreDist"))
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