package org.local.clustering

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
			val mReach = List(
				coreDistVec.value.apply(i.toInt),
				coreDistVec.value.apply(j.toInt),
				dist).max
			Edge(i, j, mReach)
		})

		val vertices = dfCartesian.select(iCol).distinct()
  		.rdd
  		.map(row => (row.getAs[Long](0), row.getAs[Long](0)))

		Graph(vertices, edges)
		}

	def fromJoinDF(dfJoin: DataFrame, iCol: String, jCol: String,
		distCol: String, coreDistICol: String, coreDistJCol: String)(ss: SparkSession):
		Graph[Long, Double] = {
		import ss.implicits._

//		val rddJoin = dfJoin.rdd.map(_.toSeq)

		val edges = dfJoin.map(row => {
			val i = row.getAs[Long](iCol)
			val j = row.getAs[Long](jCol)
			val dist = row.getAs[Double](distCol)
			val coreDistI = row.getAs[Double](coreDistICol)
			val coreDistJ = row.getAs[Double](coreDistJCol)

			val mReach = List(dist, coreDistI, coreDistJ).max
			Edge(i, j, mReach)
		})
  		.rdd

		val vertices = dfJoin.select(iCol).distinct()
  		.rdd
  		.map(row => (row.getAs[Long](iCol), row.getAs[Long](iCol)))

		Graph(vertices, edges)
		}

}
