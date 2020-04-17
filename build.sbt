name := "movieRecommend"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.14.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0"
)
