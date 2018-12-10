package clustering

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag  // for viz

/* the objective of class SpanningTree is to build the minimum spanning tree of the mutual
reachability graph. The mutual reachability graph is also computed in this class

Graph viz with zeppelin: https://stackoverflow.com/questions/38735413/graphx-visualization

TODO Encoders for scala Set available since Spark 2.3
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


	def naiveKruskal(graph: Graph[Long, Double])(ss: SparkSession): RDD[Edge[Double]] = {
		val edgesMST =  ss.sparkContext.parallelize(Seq[Edge[Double]]())
		val verticesMST = ss.sparkContext.parallelize(Seq[(VertexId, Long)]())

//		val sortedEdges = graph.edges.sortBy[Double](_.attr).cache()
//		val graphOrdered = Graph(graph.vertices, sortedEdges)
//		val graphOrdered = Graph.fromEdges(sortedEdges, 0L)

		val MST = recursiveKruskal(graph, Graph(verticesMST, edgesMST))(ss)

		MST.edges
	}

	def recursiveKruskal(orderedGraph: Graph[Long, Double],
		spanningGraph: Graph[Long, Double])(ss: SparkSession): Graph[Long, Double] = {
			// termination
			if (orderedGraph.edges.isEmpty()) {
				spanningGraph
			}
			else {
//				val edge = orderedGraph.triplets.first()
					val edge = orderedGraph.triplets.takeOrdered(1)(Ordering.by(_.attr)).apply(0)
				// if src and dst are connected: we remove the edge and continue
				if (areConnected(edge.srcId, edge.dstId, spanningGraph)){
					recursiveKruskal(orderedGraph.subgraph(triplet => triplet != edge),
						spanningGraph)(ss)
				}
				else { // adding lightest-weighted edge to the spanning tree
					// adding edge to the graph
					val newEdges = spanningGraph.edges
						.union(ss.sparkContext.parallelize(Seq(edge)))

					val newVertices = spanningGraph.vertices
						.union(ss.sparkContext.parallelize(Seq(
							(edge.srcId, edge.srcAttr),
							(edge.dstId, edge.dstAttr))))
						.distinct()

					recursiveKruskal(orderedGraph.subgraph(triplet => triplet != edge),
						Graph(newVertices, newEdges))(ss)
				}
			}
		}

	/** returns true if src and dst are connected in graph
		*
		* @param src
		* @param dst
		* @param graph
		* @return
		*/
	def areConnected[ED, VD](src: VertexId, dst: VertexId,
		graph: Graph[ED, VD]): Boolean = {
		val objectiveSeq = Seq(src, dst)

		val cc = graph.ops.connectedComponents()

		// filter vertices of interest
		val verticesFiltered = cc.vertices
			.map(_.swap)
  		.filter(tuple => objectiveSeq.contains(tuple._1)  || objectiveSeq.contains(tuple._2))

		// groupBy smallest index in connected component
		val verticesGroup = verticesFiltered.groupBy(_._1)
  		.map(kv =>  kv._2.toSeq.map(_._2))//   .intersect(objectiveSeq))
//			.flatMap(tuple => (tuple._1 +: tuple._2.toSeq.map(_._2)).intersect(objectiveSeq))

		val groupFiltered = verticesGroup.filter(t =>  objectiveSeq.intersect(t) == objectiveSeq )

		if (groupFiltered.isEmpty()) false
		else true
	}
}
