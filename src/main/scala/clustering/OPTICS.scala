package org.local.clustering

import org.apache.spark.graphx.Graph
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, SparkSession}


class OPTICS (
	private var epsilon: Double,
	private var mpts: Int,
	private var idCol: String,
	private var distance: (DenseVector, DenseVector) => Double,
	private var treeAlgo: String,
	// change to Seq[String] ?
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

	def getDistance(): (DenseVector, DenseVector) => Double = this.distance

	def setDistance(newDistance: (DenseVector, DenseVector) => Double): Unit =
		this.distance = newDistance

	def getFeatureCols(): String = this.featureCols

	def setFeatureCols(newFeatureCols: String): Unit = this.featureCols = newFeatureCols


	// core algorithm
	def run(df: DataFrame)(implicit ss: SparkSession): DataFrame = {
		import ss.implicits._


		val neighbors = new Neighbors(this.mpts)
		val dfDistance = neighbors.pointWiseDistance(
			df,
			this.idCol,
			this.featureCols,
			this.distance,
			false)
  		.select(this.idCol, this.idCol + "_2", "distance")

		val udfAgg = new NearestNeighborAgg(this.mpts, this.idCol + "_2", "distance")

		val dfCoreDists = dfDistance.groupBy(this.idCol).agg(udfAgg.toColumn as "coreDistTuple")
  		.withColumn("coreDistance", $"coreDistTuple".getField("_2"))
  		.drop("coreDistTuple")

		// outliers are samples which core distance is greater than epsilon
		val dfOutliers = dfCoreDists
			.filter(row => row.getAs[Double]("coreDistance") > this.epsilon)

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

		val mutualReachGraph = mutualReachability.fromJoinDF(
			dfDistCoreDist,
			this.idCol,
			this.idCol + "_2",
			"distance",
			"coreDistance",
			"coreDistance_2")(ss)

		// Spanning Tree
		val tree = new SpanningTree()

//		val spanningTree = tree.naivePrim(mutualReachGraph)
		val spanningTree = this.treeAlgo match {
			case "prim" => tree.naivePrim(mutualReachGraph)
			case "kruskal" => tree.naiveKruskal(mutualReachGraph)
			case _ => throw new IllegalArgumentException(s"${this.treeAlgo} is not  a valid argument for treeAlgo attribute")
		}

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


		dfClusters
	}
}


