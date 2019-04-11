import sbtassembly.{MergeStrategy, PathList}

name := "density-based_clustering"

organization := "org.local" //change to org.vcharvet?

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.apache.spark" %% "spark-core" % "2.3.1"  % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1"  % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.3.1"  % "provided",
  "org.apache.spark" %% "spark-graphx" % "2.3.1"  % "provided", 
  "graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11",
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.vegas-viz" %% "vegas" % "0.3.11",     // for dataviz
  "org.vegas-viz" %% "vegas-spark" % "0.3.11"
)

resolvers ++= Seq(
    "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
    "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
    "Java.net Maven2 Repository" at "http://download.java.net/maven/2/",
    "Artima Maven Repository" at "http://repo.artima.com/releases",
    "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
)

assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) =>
        xs map (_.toLowerCase) match {
          case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
          case _ => MergeStrategy.discard
        }
      case _ => MergeStrategy.first
}

test in assembly := {}
