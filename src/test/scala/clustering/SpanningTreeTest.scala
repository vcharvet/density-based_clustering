package org.local.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.scalatest.FlatSpec


class SpanningTreeTest extends FlatSpec{
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	implicit val ss = SparkSession.builder
  	.appName("Spanning Tree computation unit test")
  	.master("local[1]")
  	.getOrCreate()

	import ss.implicits._

	// graph with all components
	val inputDF = ss.sparkContext.parallelize(Seq[(Long, Long, Double)](
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

	// edges for subgraph with two connected components
	val inputSubgraph =  ss.sparkContext.parallelize(Seq[(Long, Long, Double)](
		(0, 1, 2),
		(0, 2, 2),
		(1, 2, 2),
		(3, 4, 5),
		(4, 5, 5),
		(3, 5, 5)))

	val vertices = ss.sparkContext.parallelize(Seq[(VertexId, Long)](
		(0, 0),
		(1, 1),
		(2, 2),
		(3, 3),
		(4, 4),
		(5, 5)))

	val graph = Graph[Long, Double](vertices, inputDF.map(
		triplet => Edge(triplet._1, triplet._2, triplet._3)))

	val subgraph =  Graph[Long, Double](vertices, inputSubgraph.map(
		triplet => Edge(triplet._1, triplet._2, triplet._3)))

	val exactMST = ss.sparkContext.parallelize(Seq[Edge[Double]](
		Edge(0, 1, 2),
		Edge(0, 2, 2),
		Edge(0, 3, 36),
		Edge(3, 5, 5),
		Edge(3, 4, 5)))

		val spanningTree = new SpanningTree()



	"4 and 5 in complete graph " should "be connected" in {
		val connected45 = spanningTree.areConnected(5l, 4l, graph)

		assertResult(true)(connected45)
	}

	"0 and 5 in complete graph" should "be connected" in {
		val connected05 = spanningTree.areConnected(0l, 5l, graph)

		assertResult(true)(connected05)
	}

	"4 and 5 in subgraph" should "be connected" in {
		val connected45 = spanningTree.areConnected(4l, 5l, subgraph)

		assertResult(true)(connected45)
	}

	"0 and 5 in subgraph" should "not be connected" in {
		val connected05 = spanningTree.areConnected(0l, 5l, subgraph)

		assertResult(false)(connected05)
	}

	"local Kruskal" should "yield" in {
		val t0 = System.nanoTime

		val edgesIterator = graph.edges.sortBy(_.attr).toLocalIterator

		val localComputedMST = spanningTree.localKruskal(edgesIterator).toSeq
		println(s"local Kruskal MST done in ${(System.nanoTime - t0) / 1e9d}")

		assertResult(exactMST.collect().map(_.attr).sum) (localComputedMST.map(_.attr).sum)

	}

	"Naive Prim algorithm" should "yield" in {
		val t0 = System.nanoTime
		val naiveComputedMST = spanningTree.naivePrim(graph)
			.sortBy(edge => (edge.srcId, edge.dstId))
		println(s"MST with prim done in ${(System.nanoTime - t0) / 1e9d} \n")
		assertResult(exactMST.collect().map(_.attr).sum) (naiveComputedMST.collect().map(_.attr).sum)
	}

	"Naive Kruskal" should "yield" in {
		val t0 = System.nanoTime
		val naiveComputedMST = spanningTree.naiveKruskal(graph)
  		.sortBy(edge => (edge.srcId, edge.dstId))
		println(s"MST with kruskal done in ${(System.nanoTime - t0) / 1e9d} \n")

//		println("Naive computed MST:")
//		naiveComputedMST.foreach(println(_))
		assertResult(exactMST.collect().map(_.attr).sum) (naiveComputedMST.collect().map(_.attr).sum)
	}

}
