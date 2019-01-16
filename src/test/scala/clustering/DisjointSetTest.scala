package org.local.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Edge
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

class DisjointSetTest extends FlatSpec{
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	implicit val ss = SparkSession.builder
		.appName("Spanning Tree computation unit test")
		.master("local[1]")
		.getOrCreate()

	val edges = ss.sparkContext.parallelize(Seq[(Long, Long, Double)](
		(0, 1, 2),
		(0, 2, 2),
		(0, 3, 36),
		(0, 4, 64),
		(0, 5, 37),
		(1, 2, 2),
		(1, 3, 37),
		(1, 4, 65),
		(1, 5, 36),
		(2, 3, 49),
		(2, 4, 81),
		(2, 5, 50),
		(3, 4, 5),
		(3, 5, 5)))
  	.map(triplet => Edge(triplet._1, triplet._2, triplet._3))


	val fruits = new DisjointSet[String]

	fruits.add("banana").add("cherry").add("strawberry")

	"fruits disjoint-set" should "yield" in {
		val expected = Map(("banana", "banana"), ("cherry", "cherry"), ("strawberry", "strawberry"))

		assertResult(expected.toSeq)(fruits.getParent)
	}

	"find banana" should "yield" in {
		val computed = fruits.find("banana")

		assertResult("banana")(computed)
	}

	"union-find" should "yield" in{
		fruits.union("banana", "cherry")
		val computed = fruits.areConnected("banana", "cherry")

		assertResult(true)(computed)

	}
}
