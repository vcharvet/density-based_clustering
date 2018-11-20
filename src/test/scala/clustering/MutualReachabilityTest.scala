package clustering

import breeze.linalg.DenseVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FlatSpec



class MutualReachabilityTest extends FlatSpec{
	Logger.getLogger("org").setLevel(Level.WARN)
	Logger.getLogger("akka").setLevel(Level.WARN)

	val ss = SparkSession.builder
		.appName("clustering.CoreDistance Unit Test")
		.master("local[1]")
		.getOrCreate()

	import ss.implicits._

	val wholeCartesian = ss.sparkContext.parallelize(Seq[(Long, Long, Double, Double)](
		(0, 1, 1, 1),
		(0, 2, 1, 1),
		(0, 3, 36, 36),
		(0, 4, 64, 64),
		(0, 5, 37, 37),
		(1, 0, 1, (2)),
		(1, 2, (2), (2)),
		(1, 3, 37, 37),
		(1, 4, 65, 65),
		(1, 5, 36, 36),
		(2, 0, 1, (2)),
		(2, 1, (2), (2)),
		(2, 3, 49, 49),
		(2, 4, 81, 81),
		(2, 5, 50, 50),
		(3, 0, 36, 36),
		(3, 1, 37, 37),
		(3, 2, 49, 49),
		(3, 4, 4, 4),
		(3, 5, 1, 4),
		(4, 0, 64, 64),
		(4, 1, 65, 65),
		(4, 2, 81, 81),
		(4, 3, 4, (5)),
		(4, 5, (5), (5)),
		(5, 0, 37, 37),
		(5, 1, 36, 36),
		(5, 2, 50, 50),
		(5, 3, 1, (5)),
		(5, 4, (5), (5))))
  	.toDF("i", "j", "distIJ", "mutualReachDist")

	val minPts = 2

	val coreDistances = DenseVector[Double](1, (2), (2), 4, 5, 5)
	val broadcastCD = ss.sparkContext.broadcast(coreDistances)

	val mutualReachability = new clustering.MutualReachabilityGraph

	val computedGraph = mutualReachability.fromCartesianDF(
		wholeCartesian,
		"i",
		broadcastCD)

//		println("Edges:")
//		computedGraph.edges.collect().foreach(println(_))
//		println("Vertices:")
//		computedGraph.vertices.collect().foreach(println(_))
	val actualEdges = wholeCartesian.select("i", "j", "mutualReachDist")
  	.rdd
  	.map(_.toSeq)
  	.map(seq => Edge(seq(0).asInstanceOf[Long], seq(1).asInstanceOf[Long], seq(2)))

	val actualVertices = ss.sparkContext.parallelize(Seq[(Long, Long)](
		(0, 0),
		(1, 1),
		(2, 2),
		(3, 3),
		(4, 4),
		(5, 5)))
  	.sortBy(t => t._1)

	val actualGraph = Graph(actualVertices, actualEdges)

	"Vertices" should "yield" in {
		assertResult(actualGraph.vertices.collect().sortBy(t => t._1))(computedGraph.vertices.collect().sortBy(t => t._1))
	}

	"Edges" should "yield" in {
		assertResult(actualGraph.edges.collect().sortBy(t => (t.srcId, t.dstId )))(computedGraph.edges.collect().sortBy(t => (t.srcId, t.dstId)))
	}


}
