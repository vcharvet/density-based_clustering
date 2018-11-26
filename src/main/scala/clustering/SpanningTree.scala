package clustering

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/* the objective of class SpanningTree is to build the minimum spanning tree of the mutual
reachability graph. The mutual reachability graph is also computed in this class

 */
class SpanningTree {
	/** implementation taken from
		* https://stackoverflow.com/questions/36831804/how-to-parallel-prims-algorithm-in-graphx/36892399#36892399
		*
		* @param Graph
		* @return
		*/
	def naivePrim(Graph: Graph[Long, Double])(ss: SparkSession): RDD[Edge[Double]] = {
		val emptyEdges = ss.sparkContext.parallelize(Seq[Edge[Double]]())
		val emptyVertices = ss.sparkContext.parallelize(Seq[(VertexId, Long)]())
//		var MST = Graph(emptyVertices, emptyEdges)
		var MST = ss.sparkContext.parallelize(Seq[Edge[Double]]())

		val bySrc = Graph.triplets.map(triplet => (triplet.srcId, triplet))
		val byDst = Graph.triplets.map(triplet => (triplet.dstId, triplet))
		// rdd for encontered vertices
		var Vt = ss.sparkContext.parallelize(Array(Graph.pickRandomVertex()))

		val vCount = Graph.vertices.count()

		while (Vt.count() < vCount){
			val hVt = Vt.map(x => (x, x))

			val bySrcJoined = bySrc.join(hVt).map(_._2._1)
			val byDstJoined = byDst.join(hVt).map(_._2._1)

			// keep triplets whose src and dst haven't been seen
			val candidates = bySrcJoined
				.union(byDstJoined)
  			.subtract(
  				byDstJoined.intersection(bySrcJoined))

			val triplet = candidates.sortBy(triplet => triplet.attr).first()

			MST = MST.union(ss.sparkContext.parallelize(Seq(Edge(triplet.srcId, triplet.dstId, triplet.attr))))

			if (!Vt.filter(x => triplet.srcId == x).isEmpty()){
				Vt = Vt.union(ss.sparkContext.parallelize(Seq(triplet.dstId)))
				}
			else {
				Vt = Vt.union(ss.sparkContext.parallelize(Seq(triplet.srcId)))
				}
		}
		MST
	}


	def NaiveKruskal(graph: Graph[Long, Double])(ss: SparkSession): RDD[Edge[Double]] = {
		var MST = ss.sparkContext.parallelize(Seq[Edge[Double]]())

		var graphOrdered = Graph(graph.vertices, graph.edges.sortBy(_.attr))



		MST
	}
}
