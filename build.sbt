import sbtassembly.{MergeStrategy, PathList}

name := "density-based_clustering"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1",
  "org.apache.spark" %% "spark-graphx" % "2.3.1", 
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

resolvers ++= Seq(
    "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
    "Apache Repository" at "https://repository.apache.org/content/repositories/releases/",
    "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
)

assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) =>
        xs map (_.toLowerCase) match {
          case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
          case _ => MergeStrategy.discard
        }
      case _ => MergeStrategy.first
}
