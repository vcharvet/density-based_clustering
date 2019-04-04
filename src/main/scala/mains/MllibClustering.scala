package mains

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{GaussianMixture, KMeans}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.local.clustering
import vegas.sparkExt._
import vegas._
import vegas.spec.Spec.MarkEnums.Point


object BasicClustering {
  /** compares clustering algorithm from mllib
    *
    * @param args
    */
  def main(args: Array[String]): Unit= {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val path = "data/datasets" + args(0)  //name of input csv file
    val n_clusters = args(1).toInt  // number of wanted clusters

    val features = "coordinates"
    val prediction = "cluster_id"
    val figSize = 600D

    // instanciate SparkSession
    implicit val ss: SparkSession = SparkConfig("main mllib test").session()
    import ss.implicits._
    // fetching df from csv file in path
    val raw_DF = ss.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv(path)
      .na.drop("any")
  		.withColumn("id", $"id"cast(LongType))
//  		.sample(false, 0.1)
			.cache()

		raw_DF.show(5)

    val nPoints = raw_DF.count()
    val cols = raw_DF.drop("id", " class").columns // (id, a0, a1, class)

    println(s"DataFrame with $nPoints rows and ${cols.length } features")

    // assembles the two feature columns
    val assembler = new VectorAssembler()
  		.setOutputCol("assembledFeatures")
      .setInputCols(cols)

		val scaler = new StandardScaler()
  		.setInputCol(assembler.getOutputCol)
  		.setOutputCol(features)

		val pipeline = new Pipeline()
  		.setStages(Array(assembler, scaler))
  		.fit(raw_DF)

    val DF = pipeline.transform(raw_DF)

    // clustering algorithms
    var  t0 = System.nanoTime
    val kMeans = new KMeans()
      .setFeaturesCol(features)
      .setK(n_clusters)
      .setPredictionCol(prediction + "_kmeans")
      .setSeed(0)
      .fit(DF)

    val clustersKMeans = kMeans.transform(DF)
		println(s"Execution time for KMeans: ${(System.nanoTime - t0) / 1e9d}")

		t0 = System.nanoTime
    val gaussianMixture = new GaussianMixture()
      .setFeaturesCol(features)
      .setK(n_clusters)
      .setPredictionCol(prediction + "_gmm")
      .setSeed(0)
      .fit(DF)

    val clustersGMM = gaussianMixture.transform(DF)
		println(s"Execution time for GMM: ${(System.nanoTime - t0) / 1e9d}")


		t0 = System.nanoTime
    val optics = new clustering.OPTICS(1.4, 2, "id", Some(Vectors.sqdist),
    	"distributed", "lsh", None, "assembledFeatures")

		val clustersOPTICS = optics.run(DF, Some(nPoints), Some(cols.length))
		println(s"Execution time for OPTICS: ${(System.nanoTime - t0) / 1e9d}")

		val clustersWithCoord = clustersOPTICS
			.join(raw_DF.select("id", cols(0), cols(1)), "id")
			clustersWithCoord.show()



    // plotting clusters
    val plot_kmeans = Vegas("Kmeans Clusters", name=s"kmeans on $nPoints",
      width=figSize, height = figSize)
      .withDataFrame(clustersKMeans)
      .mark(Point)
      .encodeX(cols(0), Quantitative)
      .encodeY(cols(1), Quantitative)
      .encodeColor(field = kMeans.getPredictionCol, dataType=Nom)


    val plot_gmm = Vegas("GMM Clusters",
      width = figSize, height = figSize)
      .withDataFrame(clustersGMM)
      .mark(Point)
      .encodeX(cols(0), Quantitative)
      .encodeY(cols(1), Quantitative)
      .encodeColor(field = gaussianMixture.getPredictionCol, dataType=Nom)
//      .show
//    println(plot_kmeans.html.pageHTML())
//    (plot_kmeans.html.plotHTML(s"KMeans on $nPoints"))
//    println(plot_gmm.html.pageHTML())
//    (plot_gmm.html.plotHTML(s"GMM on $nPoints"))

		val plot_OPTICS = Vegas("OPTICS Clusters",
      width = figSize, height = figSize)
      .withDataFrame(clustersWithCoord)
      .mark(Point)
      .encodeX(cols(0), Quantitative)
      .encodeY(cols(1), Quantitative)
      .encodeColor(field = "clusterID", dataType=Nom)
//      .show

    plot_kmeans.window.show
    plot_gmm.window.show
    plot_OPTICS.window.show

    println("Done")

  }
}