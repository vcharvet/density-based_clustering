package clustering

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.broadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/* class to compute mutual reachability graph

 */

class MutualReachabilityGraph {

	/**
		*
		* @param dfCartesian
		*        as output by Neighbors.pointWiseDistance
		* @param iCol
		* @param coreDistVec
		* @return
		*/
	def fromCartesianDF(dfCartesian: DataFrame, iCol: String,
		coreDistVec: Broadcast[DenseVector[Double]]): Graph[Long, Double] = {
		// RDD[Seq(i, j, dist(i,j))]
		val rddCartesian = dfCartesian.rdd.map(row => row.toSeq)

		val edges = rddCartesian.map(seq => {
			val i = seq(0).asInstanceOf[Long]
			val j = seq(1).asInstanceOf[Long]
			val dist = seq(2).asInstanceOf[Double]
			Edge(i, j, math.max(coreDistVec.value.apply(i.toInt), dist))
		})

		val vertices = dfCartesian.select(iCol).distinct()
  		.rdd
  		.map(row => (row.getAs[Long](0), row.getAs[Long](0)))

		Graph(vertices, edges)
		}

}
