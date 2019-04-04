package mains

import java.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType
import org.local.clustering

object TimeEvaluation {

	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val datasetName = args(0)
		val k = args(1).toInt
		val path = "data/datasets/" + datasetName + ".csv"

		// OPTICS hyperparameters
		val epsilon=1.4
		val mpts = 2

		val features = "features"
		val prediction = "clusterId"

		implicit val ss: SparkSession = SparkConfig(s"time evaluation on ${datasetName}").session()
		import ss.implicits._

		val raw_DF = ss.read
			.option("header", "true")
			.option("inferSchema", "true")
			.option("sep", ",")
			.csv(path)
			.na.drop("any")
			.withColumn("id", $"id" cast (LongType))

		val nPoints = raw_DF.count // or countApprox?

		val cols = Array("a0", "a1")

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

		// clustering with K-Means
		var t0 = System.nanoTime
		val kMeans = new KMeans()
			.setFeaturesCol(features)
			.setK(k)
			.setPredictionCol(prediction + "_kmeans")
			.setSeed(0)
			.fit(DF)

		val clustersKMeans = kMeans.transform(DF)
		val kMeansTime = (System.nanoTime - t0) / 1e9d
		println(s"Execution time for KMeans: ${kMeansTime}")

		// clustering with OPTICS
		t0 = System.nanoTime
		val optics = new clustering.OPTICS(epsilon, mpts, "id", Some(Vectors.sqdist),
			"distributed", "lsh", None, "assembledFeatures")

		val clustersOPTICS = optics.run(DF, Some(nPoints), Some(cols.length))
		val opticsTime = (System.nanoTime - t0) / 1e9d
		println(s"Execution time for OPTICS: ${opticsTime}")

		// saving results in text file
		val outPath = "data/results/times-"
		val file = new File(outPath + datasetName + ".txt")
		val bw = new BufferedWriter(new FileWriter(file))
		bw.write(s"kmeans ${kMeansTime} optics ${opticsTime}")
		bw.close()

		println("Done")
		ss.close()
	}
}

