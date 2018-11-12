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
		(4l, 5l, 4d),
		(4l, 6l, 1d),
		(5l, 4l, 4d),
		(5l, 6l, 5d),
		(6l, 4l, 1d),
		(6l, 5l, 5d)))
  	.toDF("i", "j", "distIJ")

	// group of point 4
	val dfGroup = ss.sparkContext.parallelize(Seq(
		(5l, 4d),
		(6l, 1d)))
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

		assertResult(Map((5l, 4d)))(resCol.head().get(0))
		}

	"Aggregator on 2 nearest neighbors" should "yield" in{
		val udfAgg = new NearestNeighborAgg(2, "j", "distIJ")

		val computed = grouped_df.agg(udfAgg($"j", $"distIJ").alias("kNeighborDistance") )
  			.orderBy("i")

		val expected = ss.sparkContext.parallelize(Seq(
			(4l, Map[Long, Double]((5l, 4d))),
			(5l, Map[Long, Double]((6l, 5d))),
			(6l, Map[Long, Double]((5l, 5d))))).toDF("j", "2NNeighbor")

		assertResult(expected.collect()  )(computed.collect())
		}


}
