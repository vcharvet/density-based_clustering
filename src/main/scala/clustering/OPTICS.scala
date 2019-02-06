package org.local.clustering

import org.apache.spark.graphx.Graph
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions


class OPTICS (
	private var epsilon: Double,
	private var mpts: Int,
	private var idCol: String,
	private var distance: Option[(DenseVector, DenseVector) => Double], // or nonr
	private var treeAlgo: String, //"prim" or "kruskal" or "distributed"
	private var method: String, // "exact" or "lsh'
	private var numPartitions: Option[Int],
	private var featureCols: String) extends Serializable with Logging {

//	private def this(idCol: String, distance: (DenseVector, DenseVector) => Double,
//		featureCols: Seq[String]) =
//		this(idCol, distance, featureCols)


	// getters and setters
	def getEpsilon(): Double =  this.epsilon

	def setEpsilon(newEpsilon: Double): Unit = this.epsilon = newEpsilon

	def getMpts(): Int = this.mpts

	def setMpts(newMpts: Int): Unit = this.mpts = newMpts

	def getIdCol(): String = this.idCol

	def setIdCol(newIdCol: String): Unit = this.idCol = newIdCol

	def getDistance(): Option[(DenseVector, DenseVector) => Double] = this.distance

	def setDistance(newDistance: Option[(DenseVector, DenseVector) => Double]): Unit =
		this.distance = newDistance

	def getFeatureCols(): String = this.featureCols

	def setFeatureCols(newFeatureCols: String): Unit = this.featureCols = newFeatureCols


	// core algorithm
	def run(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
		import ss.implicits._
		println(s"Running OPTICS algorithm on df ${df.toString()}")

		println("Computing pointwise distances...")
		//variable to keep track of execution time
		var t0 = System.nanoTime

		val seed = 1l
		val thresh = 20d
		val bucketLength = 5d
		val neighbors = new Neighbors(this.mpts)

		val dfDistance = neighbors.pointWiseDistance(
			df,
			this.idCol,
			this.featureCols,
			this.distance.getOrElse(Vectors.sqdist),
			false)
  		.select(this.idCol, this.idCol + "_2", "distance")

		println(s"Pointwise distances computed in ${(System.nanoTime - t0) / 1e9d}s")

		//aggregator for core-distance TODO should be replaced by Neighbors.kNearestNeighbor
		val udfAgg = new NearestNeighborAgg(this.mpts, this.idCol + "_2", "distance")

		t0 = System.nanoTime
		println("Computing core distances...")
//		val dfCoreDists = dfDistance.groupBy(this.idCol).agg(udfAgg.toColumn as "coreDistTuple")
//  		.withColumn("coreDistance", $"coreDistTuple".getField("_2"))
//  		.drop("coreDistTuple")
		val dfCoreDists = method match {
			case "exact" => neighbors.coreDistance(dfDistance, idCol, idCol + "_2", "distance")
			case "lsh" => neighbors.approximateCoreDistance(df, idCol, featureCols, seed, thresh, bucketLength, distance)
			case _ => throw new IllegalArgumentException(s"${method} is not valid argument for this.method, " +
				s"use'exact' or 'lsh' instead")
}

		println(s"Core distances done in  ${(System.nanoTime - t0) / 1e9d}s")
		// outliers are samples which core distance is greater than epsilon
		val dfOutliers = dfCoreDists
			.filter(row => row.getAs[Double]("coreDistance") > this.epsilon)
  		.withColumn("clusterID", functions.lit(-1l) )
  		.select(idCol, "clusterID")

		val dfOthers =  dfCoreDists
			.filter(row => row.getAs[Double]("coreDistance") <= this.epsilon)
  		.withColumnRenamed("id", "idCoreDist")

		// df with distances(i,j), coreDist(i), coreDist(j)
		val dfDistCoreDist = dfDistance
		.filter(row => row.getAs[Long](this.idCol) <= row.getAs[Long](this.idCol + "_2"))
		.join(dfOthers, dfDistance(this.idCol)  === $"idCoreDist", "left")
		.drop("idCoreDist")
  	.join(dfOthers.withColumnRenamed("coreDistance", "coreDistance_2"),
  		dfDistance(this.idCol + "_2") === $"idCoreDist", "left")
		.drop("idCoreDist")
//  .withColumn("distance", $"distIJ".getItem("distIJ"))

		// mutual rechability graph
		val mutualReachability = new MutualReachabilityGraph()

		t0 = System.nanoTime
		println("Computing mutual reachability graph...")
		val mutualReachGraph = mutualReachability.fromJoinDF(
			dfDistCoreDist,
			this.idCol,
			this.idCol + "_2",
			"distance",
			"coreDistance",
			"coreDistance_2" +
				"")(ss)

		println(s"Mutual reachability graph done in ${(System.nanoTime - t0) / 1e9d}s")

		// Spanning Tree
		val tree = new SpanningTree()

		t0 = System.nanoTime
		println("Computing minimum spanning tree...")
//		val spanningTree = tree.naivePrim(mutualReachGraph)
		val spanningTree = this.treeAlgo match {
			case "prim" => tree.naivePrim(mutualReachGraph)
			case "kruskal" => tree.naiveKruskal(mutualReachGraph)
			case "distributed" => tree.distributedMST(mutualReachGraph, numPartitions) //change numPartitions
			case _ => throw new IllegalArgumentException(s"${this.treeAlgo} is not  a valid argument for treeAlgo attribute")
		}
		println(s"Spanning tree computed in ${(System.nanoTime - t0) / 1e9d}s")

		val spanningGraph = Graph(mutualReachGraph.vertices, spanningTree)

		val prunedGraph = spanningGraph.subgraph(triplets => triplets.attr < this.epsilon)

		val connectedComponnents = prunedGraph.ops.connectedComponents()

		//each cc corresponds to one cluster
		val dfClusters = dfOthers
  		.rdd
  		.map(row => (row.getAs[Long]("idCoreDist"), row))
  		.join(connectedComponnents.vertices)
			.map{case (id, (row, vertexID)) => (id, vertexID)}
			.toDF(this.idCol, "clusterID")

//		println(s"OPTICS run completed - Displaying resulting df: \n")
//		dfClusters.show()


		dfClusters.union(dfOutliers)
	}
}


