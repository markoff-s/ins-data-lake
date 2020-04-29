name := "intraday-submissions-processor"

version := "0.1"

scalaVersion := "2.11.12"


// spark
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"

// hudi
// https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark-bundle
libraryDependencies += "org.apache.hudi" %% "hudi-spark-bundle" % "0.5.1-incubating"
// https://mvnrepository.com/artifact/org.apache.spark/spark-avro
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.4"